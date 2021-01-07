(ns aerospike-clj.client
  (:refer-clojure :exclude [update])
  (:require [aerospike-clj.policy :as policy]
            [aerospike-clj.utils :as utils]
            [aerospike-clj.metrics :as metrics]
            [aerospike-clj.key :as as-key]
            [aerospike-clj.listeners]
            [aerospike-clj.aerospike-record :as record]
            [promesa.core :as p])
  (:import [com.aerospike.client IAerospikeClient AerospikeClient Key Bin Operation BatchRead]
           [com.aerospike.client.async EventLoop NioEventLoops EventLoops]
           [com.aerospike.client.policy Policy BatchPolicy ClientPolicy RecordExistsAction WritePolicy ScanPolicy InfoPolicy]
           [com.aerospike.client.cluster Node]
           [com.aerospike.client Key Host]
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

(defn- create-client
  "Returns the Java `AerospikeClient` instance. To build the Clojure `IAerospikeClient` one,
  use `init-simple-aerospike-client`."
  ([host client-policy]
   (create-client host client-policy 3000))
  ([hosts client-policy port]
   (let [hosts-arr (into-array Host (for [h hosts]
                                      ^Host (Host. h port)))]
     (AerospikeClient. ^ClientPolicy client-policy ^"[Lcom.aerospike.client.Host;" hosts-arr))))

(defn create-event-loops
  "Called internally to create the event loops for the client.
  Can also be used to share event loops between several clients."
  [conf]
  (let [elp (policy/map->event-policy conf)]
    (NioEventLoops. elp 1 true "NioEventLoops")))

;; listeners
(defprotocol ClientEvents
  "Continuation functions that are registered when an async DB operation is called.
  The DB passed is an `IAerospikeClient` instance.
  The value returned from those function will be the value of the returned deferred from the async operation."
  (on-success [_ op-name op-result index op-start-time opts #_db]
    "A continuation function. Registered on the operation future and called when operations succeeds.")
  (on-failure [_ op-name op-ex index op-start-time opts #_db]
    "A continuation function. Registered on the operation future and called when operations fails."))

(defn- client-events-reducer [op-name index op-start-time opts]
  (fn [op-future client-events]
    (-> op-future
        (p/then (fn [op-result]
                  (on-success client-events op-name op-result index op-start-time opts)))
        (p/catch (fn [op-exception]
                   (on-failure client-events op-name op-exception index op-start-time opts))))))

(defn- register-events [op-future client-events op-name index op-start-time opts]
  (if (empty? client-events)
    op-future
    (reduce (client-events-reducer op-name index op-start-time opts) op-future client-events)))

(defprotocol UserKey
  "Use `create-key` directly to pass a pre-made custom key to the public API.
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
  (let [k (.key batch-read)]
    (-> (record/record->map (.record batch-read))
        (assoc :index (.toString (.userKey k)))
        (assoc :set (.setName k)))))

(def ^:private x-bin-convert
  (comp
    (map (fn [[k v]] [k (utils/sanitize-bin-value v)]))
    (map (fn [[k v]] (create-bin k v)))))

(defn- map->multiple-bins [^IPersistentMap m]
  (let [bin-names (keys m)]
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

(defprotocol IAerospikeClient2
  (get-single
    [this index set-name]
    [this index set-name conf]
    [this index set-name conf bin-names]
    "Returns a single record: `(transcoder AerospikeRecord)`. The default transcoder is `identity`.
    Pass a `:policy` in `conf` to use a non-default `ReadPolicy`")

  (get-single-no-meta
    [this index set-name]
    [this index set-name ^IPersistentVector bin-names]
    "Shorthand to return a single record payload only.")

  (get-batch
    [this batch-reads]
    [this batch-reads conf]
    "Get a batch of records from the cluster asynchronously. `batch-reads` is a collection of maps
    of the form `{:index \"foo\" :set \"someset\" :bins [...]}` the `:bins` key can have required
    bins for the batched keys or missing/[:all] to get all the bins (see `_get`). The result is a
    vector of `AerospikeRecord`s in the same order of keys. Missing keys result in `nil` in corresponding
    positions.")

  (get-multiple
    [this indices sets]
    [this indices sets conf]
    "DEPRECATED - use `get-batch` instead.

    Returns a (future) sequence of AerospikeRecords returned by `get-single`
    with records in corresponding places to the required keys. Indices and sets should be sequences.
    The `conf` map is passed to all `get-single` invocations.")

  (exists-batch
    [this indices]
    [this indices conf]
    "Check for each key in a batch if it exists. `indices` is a collection of maps
    of the form `{:index \"key-name\" :set \"set-name\"}`. The result is a
    vector of booleans in the same order as indices.")

  (exists?
    [this index set-name]
    [this index set-name conf]
    "Test if an index exists.")

  (put
    [this index set-name data expiration]
    [this index set-name data expiration conf]
    "Writes `data` into a record with the key `index`, with the ttl of `expiration` seconds.
    `index` should be string. Pass a function in `(:trascoder conf)` to modify `data` before it
    is sent to the DB.
    Pass a `WritePolicy` in `(:policy conf)` to uses the non-default policy.
    When a Clojure map is provided for the `data` argument, a multiple bin record will be created.
    Each key-value pair in the map will be treated as a bin-name, bin-value pair. Bin-names must be
    strings. Bin-values can be any nested data structure.")

  (put-multiple
    [this indices set-names payloads expirations]
    [this indices set-names payloads expirations conf]
    "Put multiple payloads by invoking `put`. All arguments should be mutually
    corresponding sequences.")

  (set-single
    [this index set-name data expiration]
    [this index set-name data expiration conf]
    "`put` with an update policy")

  (create
    [this index set-name data expiration]
    [this index set-name data expiration conf]
    "`put` with a create-only policy")

  (replace-only
    [this index set-name data expiration]
    [this index set-name data expiration conf]
    "`put` with a replace-only policy")

  (update
    [this index set-name new-record generation new-expiration]
    [this index set-name new-record generation new-expiration conf]
    "Writing a new value for the key `index`.
    Generation: the expected modification count of the record (i.e. how many times was it
    modified before my current action). Pass a function in `(:trascoder conf)` to modify
    `data` before it is sent to the DB.")

  (add-bins
    [this index set-name ^IPersistentMap new-data new-expiration]
    [this index set-name ^IPersistentMap new-data new-expiration conf]
    "Add bins to an existing record without modifying old data. The `new-data` must be a
    Clojure map.")

  (touch [this index set-name expiration]
    "Updates the ttl of the record stored under at `index` to `expiration` seconds from now.
    Expects records to exist.")

  (delete
    [this index set-name]
    [this index set-name conf]
    "Delete the record stored for key <index>.
    Returns async true/false for deletion success (hit).")

  (delete-bins
    [this index set-name ^IPersistentVector bin-names new-expiration]
    [this index set-name ^IPersistentVector bin-names new-expiration conf]
    "Delete bins from an existing record. The `bin-names` must be a vector of strings.")

  (operate
    [this index set-name expiration operations]
    [this index set-name expiration operations conf]
    "Asynchronously perform multiple read/write operations on a single key in one batch call.
    This method registers the command with an event loop and returns. The event loop thread
    will process the command and send the results to the listener.
    `commands` is a sequence of Aerospike CDT operations.")

  (scan-set [this aero-namespace set-name conf]
    "Scans through the given set and calls a user defined callback for each record that was found.
    Returns a deferred response that resolves once the scan completes. When the scan completes
    successfully it returns `true`. The scan may be aborted by returning :abort-scan from the callback.
    In that case the return value is `false`.

    The `conf` argument should be a map with the following keys:
      :callback - Function that accepts a com.aerospike.client.Value and an AerospikeRecord.
      :policy -   Optional. com.aerospike.client.policy.ScanPolicy.
      :bins -     Optional. Vector of bin names to return. Returns all bins by default.")

  (info
    [this node info-commands]
    [this node info-commands conf]
    "Asynchronously make info commands to a node. a node can be retreived from `get-nodes`. commands is a seq
    of strings available from https://www.aerospike.com/docs/reference/info/index.html the returned future conatains
    a map from and info command to its response.
    conf can contain {:policy InfoPolicy} ")

  (get-nodes [this])

  (get-cluster-stats [this]
    "For each client, return a vector of [metric-name metric-val] 2-tuples.
    The metric name is a dot separated string that should be convenient for
    reporting to statsd/graphite. All values are gauges.")

  (healthy? [this] [this operation-timeout-ms]
    "Returns true iff the cluster is reachable and can take reads and writes.
    Uses __health-check set to avoid data collisions. `operation-timeout-ms` is
    for total timeout of reads (default is 1s) including 2 retries so a small
    over estimation is advised to avoid false negatives.")

  (stop [this]
    "Gracefully stop a client, waiting until all async operations finish.
    The underlying EventLoops instance is closed iff it was not provided externally."))

(defn- ^BatchRead map->batch-read [batch-read-map dbns]
  (let [k (create-key (:index batch-read-map) dbns (:set batch-read-map))]
    (if (or (= [:all] (:bins batch-read-map))
            (nil? (:bins batch-read-map)))
      (BatchRead. k true)
      (BatchRead. k ^"[Ljava.lang.String;" (utils/v->array String (:bins batch-read-map))))))

;; put
(defn- _put [client event-loops dbns client-events index data policy set-name]
  (let [bins       (data->bins data)
        op-future  (p/deferred)
        start-time (System/nanoTime)]
    (.put ^AerospikeClient client
          ^EventLoop (.next ^EventLoops event-loops)
          (AsyncWriteListener. op-future)
          ^WritePolicy policy
          (create-key index dbns set-name)
          ^"[Lcom.aerospike.client.Bin;" bins)
    (register-events op-future client-events "write" index start-time {})))

(defrecord SimpleAerospikeClient [^AerospikeClient client
                                  ^EventLoops el
                                  dbns
                                  cluster-name
                                  client-events
                                  close-event-loops?]
  IAerospikeClient2
  (get-single [this index set-name]
    (get-single this index set-name {} :all))

  (get-single [this index set-name conf]
    (get-single this index set-name conf :all))

  (get-single [_this index set-name conf bin-names]
    (let [op-future  (p/deferred)
          start-time (System/nanoTime)]
      (if (and (identical? :all bin-names)
               (not (utils/single-bin? bin-names)))
        ;; When :all is passed as an argument for bin-names and there is more than one bin,
        ;; the `get` method does not require bin-names and the whole record is retrieved
        (.get client
              ^EventLoop (.next el)
              (AsyncRecordListener. op-future)
              ^Policy (:policy conf)
              (create-key index dbns set-name))
        ;; For all other cases, bin-names are passed to a different `get` method
        (.get ^AerospikeClient client
              ^EventLoop (.next el)
              (AsyncRecordListener. op-future)
              ^Policy (:policy conf)
              (create-key index dbns set-name)
              ^"[Ljava.lang.String;" (utils/v->array String bin-names)))
      (let [p (p/chain op-future
                       record/record->map
                       (:transcoder conf identity))]
        (register-events p client-events "read" index start-time {}))))

  (get-single-no-meta [this index set-name]
    (get-single this index set-name {:transcoder :payload}))

  (get-single-no-meta [this index set-name bin-names]
    (get-single this index set-name {:transcoder :payload} bin-names))

  (get-batch [this batch-reads]
    (get-batch this batch-reads {}))

  (get-batch [_this batch-reads conf]
    (let [op-future       (p/deferred)
          start-time      (System/nanoTime)
          batch-reads-arr (ArrayList. ^Collection (mapv #(map->batch-read % dbns) batch-reads))]
      (.get client
            ^EventLoop (.next el)
            (AsyncBatchListListener. op-future)
            ^BatchPolicy (:policy conf)
            ^List batch-reads-arr)
      (let [d (p/chain op-future
                       #(mapv batch-read->map %)
                       (:transcoder conf identity))]
        (register-events d client-events "read-batch" nil start-time {}))))

  (get-multiple [this indices sets]
    (get-multiple this indices sets {}))

  (get-multiple [db indices sets conf]
    (p/all
      (map (fn [[index set-name]] (get-single db index set-name conf))
           (map vector indices sets))))

  (exists-batch [this indices]
    (exists-batch this indices {}))

  (exists-batch [this indices conf]
    (let [op-future      (p/deferred)
          start-time     (System/nanoTime)
          aero-namespace (:dbns this)
          indices        (utils/v->array Key (mapv #(create-key (:index %) aero-namespace (:set %)) indices))]
      (.exists client
               ^EventLoop (.next (:el this))
               (AsyncExistsArrayListener. op-future)
               ^BatchPolicy (:policy conf)
               ^"[Lcom.aerospike.client.Key;" indices)
      (let [d (p/chain op-future
                       vec
                       (:transcoder conf identity))]
        (register-events d client-events "exists-batch" nil start-time {}))))

  (exists? [this index set-name]
    (exists? this index set-name {}))

  (exists? [this index set-name conf]
    (let [op-future  (p/deferred)
          start-time (System/nanoTime)]
      (.exists ^AerospikeClient client
               ^EventLoop (.next (:el this))
               (AsyncExistsListener. op-future)
               ^Policy (:policy conf)
               (create-key index (:dbns this) set-name))
      (register-events op-future client-events "exists" index start-time {})))

  (put [this index set-name data expiration]
    (put this index set-name data expiration {}))

  (put [_this index set-name data expiration conf]
    (_put client
          el
          dbns
          client-events
          index
          ((:transcoder conf identity) data)
          (:policy conf (policy/write-policy client expiration))
          set-name))

  (put-multiple [this indices set-names payloads expirations]
    (put-multiple this indices set-names payloads expirations {}))

  (put-multiple [this indices set-names payloads expirations conf]
    (p/all
      (map (fn [[index set-name payload expiration]]
             (put this index set-name payload expiration conf))
           (map vector indices set-names payloads expirations))))

  (set-single [this index set-name data expiration]
    (set-single this index set-name data expiration {}))

  (set-single [_this index set-name data expiration conf]
    (_put client
          el
          dbns
          client-events
          index
          ((:transcoder conf identity) data)
          (policy/set-policy client expiration)
          set-name))

  (create [this index set-name data expiration]
    (create this index set-name data expiration {}))

  (create [_this index set-name data expiration conf]
    (_put client
          el
          dbns
          client-events
          index
          ((:transcoder conf identity) data)
          (policy/create-only-policy client expiration)
          set-name))

  (replace-only [this index set-name data expiration]
    (replace-only this index set-name data expiration {}))

  (replace-only [_this index set-name data expiration conf]
    (_put client
          el
          dbns
          client-events
          index
          ((:transcoder conf identity) data)
          (policy/replace-only-policy client expiration)
          set-name))

  (update [this index set-name new-record generation new-expiration]
    (update this index set-name new-record generation new-expiration {}))

  (update [_this index set-name new-record generation new-expiration conf]
    (_put client
          el
          dbns
          client-events
          index
          ((:transcoder conf identity) new-record)
          (policy/update-policy client generation new-expiration)
          set-name))

  (add-bins [this index set-name new-data new-expiration]
    (add-bins this index set-name new-data new-expiration {}))

  (add-bins [_this index set-name new-data new-expiration conf]
    (_put client
          el
          dbns
          client-events
          index
          ((:transcoder conf identity) new-data)
          (policy/update-only-policy client new-expiration)
          set-name))

  (touch [this index set-name expiration]
    (let [op-future  (p/deferred)
          start-time (System/nanoTime)]
      (.touch client
              ^EventLoop (.next (:el this))
              (AsyncWriteListener. op-future)
              ^WritePolicy (policy/write-policy client expiration RecordExistsAction/UPDATE_ONLY)
              (create-key index (:dbns this) set-name))
      (register-events op-future client-events "touch" index start-time {})))

  (delete [this index set-name]
    (delete this index set-name {}))

  (delete [this index set-name conf]
    (let [op-future  (p/deferred)
          start-time (System/nanoTime)]
      (.delete client
               ^EventLoop (.next (:el this))
               (AsyncDeleteListener. op-future)
               ^WritePolicy (:policy conf)
               (create-key index (:dbns this) set-name))
      (register-events op-future client-events "delete" index start-time {})))

  (delete-bins [this index set-name bin-names new-expiration]
    (delete-bins this index set-name bin-names new-expiration {}))

  (delete-bins [_this index set-name bin-names new-expiration conf]
    (let [bin-names  ((:transcoder conf identity) bin-names)
          policy     (policy/update-only-policy client new-expiration)
          op-future  (p/deferred)
          start-time (System/nanoTime)]
      (.put client
            ^EventLoop (.next el)
            (AsyncWriteListener. op-future)
            ^WritePolicy policy
            (create-key index dbns set-name)
            ^"[Lcom.aerospike.client.Bin;" (utils/v->array Bin (mapv set-bin-as-null bin-names)))
      (register-events op-future client-events "write" index start-time {})))

  (operate [this index set-name expiration operations]
    (operate this index set-name expiration operations {}))

  (operate [this index set-name expiration operations conf]
    (if (empty? operations)
      (p/resolved nil)
      (let [op-future  (p/deferred)
            start-time (System/nanoTime)]
        (.operate client
                  ^EventLoop (.next (:el this))
                  (AsyncRecordListener. op-future)
                  ^WritePolicy (:policy conf (policy/write-policy client expiration RecordExistsAction/UPDATE))
                  (create-key index (:dbns this) set-name)
                  (utils/v->array Operation operations))
        (register-events (p/then op-future record/record->map) client-events "operate" index start-time {}))))

  (scan-set [this aero-namespace set-name conf]
    (when-not (fn? (:callback conf))
      (throw (IllegalArgumentException. "(:callback conf) must be a function")))
    (let [op-future  (p/deferred)
          start-time (System/nanoTime)
          bin-names  (:bins conf)]
      (.scanAll client
                ^EventLoop (.next (:el this))
                (AsyncRecordSequenceListener. op-future (:callback conf))
                ^Policy (:policy conf (ScanPolicy.))
                aero-namespace
                set-name
                (when bin-names ^"[Ljava.lang.String;" (utils/v->array String bin-names)))
      (register-events op-future client-events "scan" nil start-time {})))

  (info [this node info-commands]
    (info this node info-commands {}))

  (info [this node info-commands conf]
    (let [op-future  (p/deferred)
          start-time (System/nanoTime)]
      (.info client
             ^EventLoop (.next (:el this))
             (AsyncInfoListener. op-future)
             ^InfoPolicy (:policy conf (.infoPolicyDefault ^AerospikeClient client))
             ^Node node
             (into-array String info-commands))
      (register-events op-future client-events "info" nil start-time {})))

  (get-nodes [_this]
    (.getNodes client))

  (get-cluster-stats [_this]
    (-> (.getClusterStats client)
        metrics/construct-cluster-metrics
        metrics/cluster-metrics->dotted)
    #_(let [clients (get-clients selector)]
        (->> clients
             (mapv #(metrics/construct-cluster-metrics (.getClusterStats ^AerospikeClient %)))
             (mapv metrics/cluster-metrics->dotted))))

  (healthy? [this]
    (healthy? this 1000))

  (healthy? [this operation-timeout-ms]
    (let [read-policy (let [p (.readPolicyDefault client)]
                        (set! (.totalTimeout p) operation-timeout-ms)
                        p)
          k           (str "__health__" (rand-int Integer/MAX_VALUE))
          v           (rand-int Integer/MAX_VALUE)
          ttl         (min 1 (int (/ operation-timeout-ms 1000)))
          set-name    "__health-check"]
      (try
        @(put this k set-name v ttl)
        (= v
           @(get-single this k set-name {:transcoder :payload
                                         :policy     read-policy}))
        (catch Exception _ex
          false))))

  (stop [_this] ;; HOW TO HANDLE MULTI-CLUSTERS???
    (println ";; Stopping aerospike clients")
    (.close client)
    (when close-event-loops?
      (.close ^EventLoops el))))

(defn expiry-unix
  "Used to convert Aerospike style returned TTLS to standard UNIX EPOCH."
  [ttl]
  (+ ttl EPOCH))

(defn init-simple-aerospike-client
  "`hosts` should be a seq of known hosts to bootstrap from.

  Optional `conf` map _can_ have:
  - :event-loops - a client compatible EventLoops instance.
    If no EventLoops instance is provided,
    a single-threaded one of type NioEventLoops is created automatically.
    The EventLoops instance for this client will be closed
    during `stop` iff it was not provided externally.
    In this case it's a responsibility of the owner to close it properly.

  Client policy configuration keys (see policy/create-client-policy)
  - :client-policy - a ready ClientPolicy
  - \"username\"
  - :port (default is 3000)
  - :client-events an implementation of ClientEvents. Either a single one or a vector
    thereof. In the case of a vector, the client will chain the instances by order."
  ([hosts aero-ns]
   (init-simple-aerospike-client hosts aero-ns {}))
  ([hosts aero-ns conf]
   (let [cluster-name       (utils/cluster-name hosts)
         close-event-loops? (nil? (:event-loops conf))
         event-loops        (or (:event-loops conf) (create-event-loops conf))
         client-policy      (:client-policy conf (policy/create-client-policy event-loops conf))]
     (println (format ";; Starting aerospike clients for clusters %s with username %s" cluster-name (get conf "username")))
     (map->SimpleAerospikeClient {:client             (create-client hosts client-policy (:port conf 3000))
                                  :el                 event-loops
                                  :dbns               aero-ns
                                  :cluster-name       cluster-name
                                  :client-events      (utils/vectorize (:client-events conf))
                                  :close-event-loops? close-event-loops?}))))
