(ns aerospike-clj.client
  (:refer-clojure :exclude [update])
  (:require [aerospike-clj.policy :as policy]
            [aerospike-clj.utils :as utils]
            [aerospike-clj.metrics :as metrics]
            [aerospike-clj.key :as as-key]
            [aerospike-clj.listeners]
            [aerospike-clj.aerospike-record :as record]
            [promesa.core :as p])
  (:import [com.aerospike.client AerospikeClient Host Key Bin Operation BatchRead]
           [com.aerospike.client.async EventLoop NioEventLoops]
           [com.aerospike.client.policy Policy BatchPolicy ClientPolicy RecordExistsAction WritePolicy ScanPolicy InfoPolicy]
           [com.aerospike.client.cluster Node]
           [aerospike_clj.listeners AsyncExistsListener AsyncDeleteListener AsyncWriteListener 
            AsyncInfoListener AsyncRecordListener AsyncRecordSequenceListener AsyncBatchListListener
            AsyncExistsArrayListener]
           [clojure.lang IPersistentMap IPersistentVector]
           [java.util List Collection ArrayList]
           [java.time Instant]))

(def EPOCH
  ^{:doc "The 0 date reference for returned record TTL"}
  (.getEpochSecond (Instant/parse "2010-01-01T00:00:00Z")))


(def MAX_BIN_NAME_LENGTH 14)

(defprotocol IAerospikeClient
  (^AerospikeClient get-client [ac] [ac index] "Returns the relevant AerospikeClient object for the specific shard")
  (get-all-clients [_] "Returns a sequence of all AerospikeClient objects."))

(defrecord SimpleAerospikeClient [^AerospikeClient ac
                                  ^NioEventLoops el
                                  ^String dbns
                                  ^String cluster-name
                                  client-events]
  IAerospikeClient
  (get-client ^AerospikeClient [_ _] ac)
  (get-client ^AerospikeClient [_] ac)
  (get-all-clients [_] [ac]))

(defn create-client
  "Returns the Java `AerospikeClient` instance. To build the Clojure `IAerospikeClient` one,
  use `init-simple-aerospike-client`."
  ([host client-policy]
   (create-client host client-policy 3000))
  ([hosts client-policy port]
   (let [hosts-arr (into-array Host (for [h hosts]
                                      ^Host (Host. h port)))]
     (AerospikeClient. ^ClientPolicy client-policy ^"[Lcom.aerospike.client.Host;" hosts-arr))))

(defn create-event-loops
  "Called internally to create the event loops of for the client.
  Can also be used to share event loops between several clients."
  [conf]
  (let [elp (policy/map->event-policy conf)]
    (NioEventLoops. elp 1 true "NioEventLoops")))

(defn init-simple-aerospike-client
  "hosts should be a seq of known hosts to bootstrap from. Optional conf map _can_ have:
  :event-loops - a client compatible event loop instace
  client policy configuration keys (see policy/create-client-policy)
  :client-policy - a ready ClientPolicy
  \"username\"
  :port (default is 3000)
  :client-events an implementation of ClientEvents. Either a single one or a vector
  thereof. In the case of a vector, the client will chain the instances by order."
  ([hosts aero-ns]
   (init-simple-aerospike-client hosts aero-ns {}))
  ([hosts aero-ns conf]
   (let [cluster-name (utils/cluster-name hosts)
         event-loops (:event-loops conf (create-event-loops conf))
         client-policy (:client-policy conf (policy/create-client-policy event-loops conf))]
     (println (format ";; Starting aerospike clients for clusters %s with username %s" cluster-name (get conf "username")))
     (map->SimpleAerospikeClient {:ac (create-client hosts client-policy (:port conf 3000))
                                  :el event-loops
                                  :dbns aero-ns
                                  :cluster-name cluster-name
                                  :client-events (utils/vectorize (:client-events conf))}))))

(defn stop-aerospike-client
  "gracefully stop a client, waiting until all async operations finish."
  [db]
  (println ";; Stopping aerospike clients")
  (doseq [^AerospikeClient client (get-all-clients db)]
    (.close client))
  (.close ^NioEventLoops (:el db)))

;; listeners
(defprotocol ClientEvents
  "Continuation functions that are registered when an async DB operation is called.
  The DB passed is an `IAerospikeClient` instance.
  The value returned from those function will be the value of the returned deferred from the async operation."
  (on-success [_ op-name op-result index op-start-time db]
              "A continuation function. Registered on the operation future and called when operations succeeds.")
  (on-failure [_ op-name op-ex     index op-start-time db]
              "A continuation function. Registered on the operation future and called when operations fails."))

(defn- register-events [op-future db op-name index op-start-time]
  (let [reducer 
        (fn
          ([] op-future)
          ([op-future ce]
           (-> op-future
               (p/then (fn [op-result]
                         (on-success  ce op-name op-result index op-start-time db))) 
               (p/catch (fn [op-result]
                          (on-success ce op-name op-result index op-start-time db))))))]
    (reduce reducer op-future (:client-events db))))
(defprotocol UserKey
  "Use `create-key` directly to pass a premade custom key to the public API.
  When passing a simple String/Integer/Long/ByteArray the key will be created
  automatically for you. If you pass a ready made key, `as-namespace` and 
  `set-name` are ignored in API calls."
  (create-key ^Key [this as-namespace set-name]))

(extend-protocol UserKey
  Key
  (create-key ^Key [this _ _]
    this)
  Object
  (create-key ^Key [this as-namespace set-name]
    (as-key/create-key this as-namespace set-name)))
  
(defn- ^Bin create-bin [^String bin-name bin-value]
  (when (< MAX_BIN_NAME_LENGTH (.length bin-name))
    (throw (Exception. (format "%s is %s characters. Bin names have to be <= 14 characters..." bin-name (.length bin-name)))))
  (Bin. bin-name bin-value))

(defn- ^Bin set-bin-as-null [^String bin-name]
  (when (< MAX_BIN_NAME_LENGTH (.length bin-name))
    (throw (Exception. (format "%s is %s characters. Bin names have to be <= 14 characters..." bin-name (.length bin-name)))))
  (Bin/asNull bin-name))

(defn- batch-read->map [^BatchRead batch-read]
  (assoc (record/record->map (.record batch-read))
         :index
         (.toString (.userKey (.key batch-read)))))

(def ^:private x-bin-convert
  (comp
    (map (fn [[k v]] [k (utils/sanitize-bin-value v)]))
    (map (fn [[k v]] (create-bin k v)))))

(defn- map->multiple-bins [^IPersistentMap m]
  (let [bin-names  (keys m)]
    (if (utils/string-keys? bin-names)
      (->> (into [] x-bin-convert m)
           (utils/v->array Bin))
      (throw (Exception. (format "Aerospike only accepts string values as bin names. Please ensure all keys in the map are strings."))))))

(defn- data->bins
  "Function to identify whether `data` will be stored as a single or multiple bin record.
  Only Clojure maps will default to multiple bins. Nested data structures are supported."
  [data]
  (if (map? data)
    (map->multiple-bins data)
    (utils/v->array Bin [^Bin (Bin. "" (utils/sanitize-bin-value data))])))

(defn _get [db index set-name conf bin-names]
  (let [client (get-client db index)
        op-future (p/deferred)
        start-time (System/nanoTime)]
    (if (and (= [:all] bin-names)
             (not (utils/single-bin? bin-names)))
      ;; When [:all] is passed as an argument for bin-names and there is more than one bin,
      ;; the `get` method does not require bin-names and the whole record is retrieved
      (.get ^AerospikeClient client
            ^EventLoop (.next ^NioEventLoops (:el db))
            (AsyncRecordListener. op-future)
            ^Policy (:policy conf)
            (create-key index (:dbns db) set-name))
      ;; For all other cases, bin-names are passed to a different `get` method
      (.get ^AerospikeClient client
            ^EventLoop (.next ^NioEventLoops (:el db))
            (AsyncRecordListener. op-future)
            ^Policy (:policy conf)
            (create-key index (:dbns db) set-name)
            ^"[Ljava.lang.String;" (utils/v->array String bin-names)))
    (let [p (p/chain op-future
                    record/record->map
                    (:transcoder conf identity))]
      (register-events p db "read" index start-time))))

(defn get-single
  "Returns a single record: `(transcoder AerospikeRecord)`. The default transcoder is `identity`.
  Pass a `:policy` in `conf` to use a non-default `ReadPolicy`"
  ([db index set-name] (_get db index set-name {} [:all]))
  ([db index set-name conf] (_get db index set-name conf [:all]))
  ([db index set-name conf bin-names] (_get db index set-name conf bin-names)))

(defn- ^BatchRead map->batch-read [batch-read-map dbns]
  (let [k (create-key (:index batch-read-map) dbns (:set batch-read-map))]
    (if (or (= [:all] (:bins batch-read-map))
            (nil? (:bins batch-read-map)))
      (BatchRead. k true)
      (BatchRead. k ^"[Ljava.lang.String;" (utils/v->array String (:bins batch-read-map))))))

(defn get-batch
  "Get a batch of records from the cluster asynchronously. `batch-reads` is a collection of maps
  of the form `{:index \"foo\" :set \"someset\" :bins [...]}` the `:bins` key can have required
  bins for the batched keys or missing/[:all] to get all the bins (see `_get`). The result is a
  vector of `AerospikeRecord`s in the same order of keys. Missing keys result in `nil` in corresponding
  positions."
  ([db batch-reads]
   (get-batch db batch-reads {}))
  ([db batch-reads conf]
   (let [client (get-client db (:index (first batch-reads)))
         op-future (p/deferred)
         start-time (System/nanoTime)
         batch-reads-arr (ArrayList. ^Collection (mapv #(map->batch-read % (:dbns db)) batch-reads))]
     (.get ^AerospikeClient client
           ^EventLoop (.next ^NioEventLoops (:el db))
           (AsyncBatchListListener. op-future)
           ^BatchPolicy (:policy conf)
           ^List batch-reads-arr)
     (let [d (p/chain op-future
                      #(mapv batch-read->map %)
                      (:transcoder conf identity))]
      (register-events d db "read-batch" nil start-time)))))

(defn exists-batch
  "Check for each key in a batch if it exists. `indices` is a collection of maps
  of the form `{:index \"key-name\" :set \"set-name\"}`. The result is a
  vector of booleans in the same order as indices."
  ([db indices]
   (exists-batch db indices {}))
  ([db indices conf]
   (let [client (get-client db)
         op-future (p/deferred)
         start-time (System/nanoTime)
         aero-namespace (:dbns db)
         indices (utils/v->array Key (mapv #(create-key (:index %) aero-namespace (:set %)) indices))]
     (.exists ^AerospikeClient client
              ^EventLoop (.next ^NioEventLoops (:el db))
              (AsyncExistsArrayListener. op-future)
              ^BatchPolicy (:policy conf)
              ^"[Lcom.aerospike.client.Key;" indices)
     (let [d (p/chain op-future
                       vec
                       (:transcoder conf identity))]
       (register-events d db "exists-batch" nil start-time)))))

(defn get-multiple
  "DEPRECATED - use `get-batch` instead.

  Returns a (future) sequence of AerospikeRecords returned by `get-single`
  with records in corresponding places to the required keys. Indices and sets should be sequences.
  The `conf` map is passed to all `get-single` invocations."
  {:deprecated "0.3.2"}
  ([db indices sets]
   (get-multiple db indices sets {}))
  ([db indices sets conf]
   (p/all
     (map (fn [[index set-name]] (get-single db index set-name conf))
          (map vector indices sets)))))

(defn exists?
  "Test if an index exists."
  ([db index set-name] (exists? db index set-name {}))
  ([db index set-name conf]
   (let [client (get-client db index)
         op-future (p/deferred)
         start-time (System/nanoTime)]
     (.exists ^AerospikeClient client
              ^EventLoop (.next ^NioEventLoops (:el db))
              (AsyncExistsListener. op-future)
              ^Policy (:policy conf)
              (create-key index (:dbns db) set-name))
     (register-events op-future db "exists" index start-time))))

(defn get-single-no-meta
  "Shorthand to return a single record payload only."
  ([db index set-name] (get-single db index set-name {:transcoder :payload}))
  ([db index set-name ^IPersistentVector bin-names]
   (get-single db index set-name {:transcoder :payload} bin-names)))

;; put
(defn- _put [db index data policy set-name]
  (let [client (get-client db index)
        bins (data->bins data)
        op-future (p/deferred)
        start-time (System/nanoTime)]
    (.put ^AerospikeClient client
          ^EventLoop (.next ^NioEventLoops (:el db))
          (AsyncWriteListener. op-future)
          ^WritePolicy policy
          (create-key index (:dbns db) set-name)
          ^"[Lcom.aerospike.client.Bin;" bins)
    (register-events op-future db "write" index start-time)))

(defn put
  "Writes `data` into a record with the key `index`, with the ttl of `expiration` seconds.
  `index` should be string. Pass a function in `(:trascoder conf)` to modify `data` before it
  is sent to the DB.
  Pass a `WritePolicy` in `(:policy conf)` to uses the non-default policy.
  When a Clojure map is provided for the `data` argument, a multiple bin record will be created.
  Each key-value pair in the map will be treated as a bin-name, bin-value pair. Bin-names must be
  strings. Bin-values can be any nested data structure."
  ([db index set-name data expiration] (put db index set-name data expiration {}))
  ([db index set-name data expiration conf]
   (_put db
         index
         ((:transcoder conf identity) data)
         (:policy conf (policy/write-policy (get-client db) expiration))
         set-name)))

(defn put-multiple
  "Put multiple payloads by invoking `put`. All arguments should be mutually
  corresponding sequences."
  ([db indices set-names payloads expirations]
   (put-multiple db indices set-names payloads expirations {}))
  ([db indices set-names payloads expirations conf]
   (p/all
     (map (fn [[index set-name payload expiration]]
            (put db index set-name payload expiration conf))
          (map vector indices set-names payloads expirations)))))

(defn set-single
  "`put` with an update policy"
  ([db index set-name data expiration]
   (set-single db index set-name data expiration {}))
  ([db index set-name data expiration conf]
   (_put db
         index
         ((:transcoder conf identity) data)
         (policy/set-policy (get-client db) expiration)
         set-name)))

(defn create
  "`put` with a create-only policy"
  ([db index set-name data expiration]
   (create db index set-name data expiration {}))
  ([db index set-name data expiration conf]
   (_put db
         index
         ((:transcoder conf identity) data)
         (policy/create-only-policy (get-client db) expiration)
         set-name)))

(defn replace-only
  "`put` with a replace-only policy"
  ([db index set-name data expiration]
   (replace-only db index set-name data expiration {}))
  ([db index set-name data expiration conf]
   (_put db
         index
         ((:transcoder conf identity) data)
         (policy/replace-only-policy (get-client db) expiration)
         set-name)))

(defn update
  "Writing a new value for the key `index`.
  Generation: the expected modification count of the record (i.e. how many times was it
  modified before my current action). Pass a function in `(:trascoder conf)` to modify
  `data` before it is sent to the DB."
  ([db index set-name new-record generation new-expiration]
   (update db index set-name new-record generation new-expiration {}))
  ([db index set-name new-record generation new-expiration conf]
   (_put db
         index
         ((:transcoder conf identity) new-record)
         (policy/update-policy (get-client db) generation new-expiration)
         set-name)))

(defn add-bins
  "Add bins to an existing record without modifying old data. The `new-data` must be a
  Clojure map."
  ([db index set-name ^IPersistentMap new-data new-expiration]
   (add-bins db index set-name new-data new-expiration {}))
  ([db index set-name ^IPersistentMap new-data new-expiration conf]
   (_put db
         index
         ((:transcoder conf identity) new-data)
         (policy/update-only-policy (get-client db) new-expiration)
         set-name)))

(defn touch
  "Updates the ttl of the record stored under at `index` to `expiration` seconds from now.
  Expects records to exist."
  [db index set-name expiration]
  (let [client (get-client db index)
        op-future (p/deferred)
        start-time (System/nanoTime)]
    (.touch ^AerospikeClient client
            ^EventLoop (.next ^NioEventLoops (:el db))
            (AsyncWriteListener. op-future)
            ^WritePolicy (policy/write-policy client expiration RecordExistsAction/UPDATE_ONLY)
            (create-key index (:dbns db) set-name))
    (register-events op-future db "touch" index start-time)))

;; delete

(defn delete
  "Delete the record stored for key <index>.
  Returns async true/false for deletion success (hit)."
  ([db index set-name]
   (delete db index set-name {}))
  ([db index set-name conf]
   (let [client (get-client db index)
         op-future (p/deferred)
         start-time (System/nanoTime)]
     (.delete ^AerospikeClient client
              ^EventLoop (.next ^NioEventLoops (:el db))
              (AsyncDeleteListener. op-future)
              ^WritePolicy (:policy conf)
              (create-key index (:dbns db) set-name))
     (register-events op-future db "delete" index start-time))))

(defn- _delete-bins [db index bin-names policy set-name]
  (let [client (get-client db index)
        op-future (p/deferred)
        start-time (System/nanoTime)]
    (.put ^AerospikeClient client
          ^EventLoop (.next ^NioEventLoops (:el db))
          (AsyncWriteListener. op-future)
          ^WritePolicy policy
          (create-key index (:dbns db) set-name)
          ^"[Lcom.aerospike.client.Bin;" (utils/v->array Bin (mapv set-bin-as-null bin-names)))
    (register-events op-future db "write" index start-time)))

(defn delete-bins
  "Delete bins from an existing record. The `bin-names` must be a vector of strings."
  ([db index set-name ^IPersistentVector bin-names new-expiration]
   (delete-bins db index set-name bin-names new-expiration {}))
  ([db index set-name ^IPersistentVector bin-names new-expiration conf]
   (_delete-bins db
                 index
                 ((:transcoder conf identity) bin-names)
                 (policy/update-only-policy (get-client db) new-expiration)
                 set-name)))

;; operate

(defn operate
  "Asynchronously perform multiple read/write operations on a single key in one batch call.
  This method registers the command with an event loop and returns. The event loop thread
  will process the command and send the results to the listener.
  `commands` is a sequence of Aerospike CDT operations."
  ([db index set-name expiration operations]
   (operate db index set-name expiration operations {}))
  ([db index set-name expiration operations conf]
   (if (empty? operations)
     (p/resolved nil)
     (let [client (get-client db index)
           op-future (p/deferred)
           start-time (System/nanoTime)]
       (.operate ^AerospikeClient client
                 ^EventLoop (.next ^NioEventLoops (:el db))
                 (AsyncRecordListener. op-future)
                 ^WritePolicy (:policy conf (policy/write-policy client expiration RecordExistsAction/UPDATE))
                 (create-key index (:dbns db) set-name)
                 (utils/v->array Operation operations))
       (register-events (p/then op-future record/record->map) db "operate" index start-time)))))

(defn scan-set
  "Scans through the given set and calls a user defined callback for each record that was found.
  Returns a deferred response that resolves once the scan completes. When the scan completes
  successfully it returns `true`. The scan may be aborted by returning :abort-scan from the callback.
  In that case the return value is `false`.

  The `conf` argument should be a map with the following keys:
  :callback - Function that accepts a com.aerospike.client.Value and an AerospikeRecord.
  :policy -   Optional. com.aerospike.client.policy.ScanPolicy.
  :bins -     Optional. Vector of bin names to return. Returns all bins by default."
  [db aero-namespace set-name conf]
  (when-not (fn? (:callback conf))
    (throw (IllegalArgumentException. "(:callback conf) must be a function")))

  (let [client (get-client db)
        op-future (p/deferred)
        start-time (System/nanoTime)
        bin-names (:bins conf)]
    (.scanAll ^AerospikeClient client
              ^EventLoop (.next ^NioEventLoops (:el db))
              (AsyncRecordSequenceListener. op-future (:callback conf))
              ^Policy (:policy conf (ScanPolicy.))
              aero-namespace
              set-name
              (when bin-names ^"[Ljava.lang.String;" (utils/v->array String bin-names)))
    (register-events op-future db "scan" nil start-time)))

(defn info 
  "Asynchronously make info commands to a node. a node can be retreived from `get-nodes`. commands is a seq
  of strings available from https://www.aerospike.com/docs/reference/info/index.html the returned future conatains
  a map from and info command to its response.
  conf can contain {:policy InfoPolicy} "
  ([db node info-commands]
   (info db node info-commands {}))
  ([db ^Node node info-commands conf]
   (let [client (get-client db)
         op-future (p/deferred)
         start-time (System/nanoTime)]
     (.info ^AerospikeClient client
            ^EventLoop (.next ^NioEventLoops (:el db))
            (AsyncInfoListener. op-future)
            ^InfoPolicy (:policy conf (.infoPolicyDefault ^AerospikeClient client))
            node
            (into-array String info-commands))
     (register-events op-future db "info" nil start-time))))

(defn get-nodes [db]
  (.getNodes (get-client db)))

;; metrics
(defn get-cluster-stats
  "For each client, return a vector of [metric-name metric-val] 2-tuples.
  The metric name is a dot separated string that should be convenient for
  reporting to statsd/graphite. All values are gauges."
  [db]
  (let [clients (get-all-clients db)]
    (->> clients
         (mapv #(metrics/construct-cluster-metrics (.getClusterStats ^AerospikeClient %)))
         (mapv metrics/cluster-metrics->dotted))))

;; health

(defn healthy?
  "Returns true iff the cluster is reachable and can take reads and writes.
  Uses __health-check set to avoid data collisions. `operation-timeout-ms` is
  for total timeout of reads (default is 1s) including 2 retries so a small
  over estimation is advised to avoid false negatives."
  ([db]
   (healthy? db 1000))
  ([db operation-timeout-ms]
   (let [read-policy (let [p (.readPolicyDefault ^AerospikeClient (get-client db ""))]
                       (set! (.totalTimeout p) operation-timeout-ms)
                       p)
         k (str "__health__" (rand-int Integer/MAX_VALUE))
         v (rand-int Integer/MAX_VALUE)
         ttl (min 1 (int (/ operation-timeout-ms 1000)))
         set-name "__health-check"]
     (try
       @(put db k set-name v ttl)
       (= v
          @(get-single db k set-name {:transcoder :payload
                                      :policy read-policy}))
       (catch Exception _ex
         false)))))

;; etc

(defn expiry-unix
  "Used to convert Aerospike style returned TTLS to standard UNIX EPOCH."
  [ttl]
  (+ ttl EPOCH))
