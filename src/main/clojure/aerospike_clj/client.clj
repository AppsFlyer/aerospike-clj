(ns aerospike-clj.client
  (:refer-clojure :exclude [update])
  (:require [promesa.core :as p]
            [aerospike-clj.policy :as policy]
            [aerospike-clj.utils :as utils]
            [aerospike-clj.metrics :as metrics]
            [aerospike-clj.key :as as-key]
            [aerospike-clj.listeners]
            [aerospike-clj.aerospike-record :as record]
            [aerospike-clj.protocols :as pt])
  (:import [java.time Instant]
           [java.util List Collection ArrayList]
           [clojure.lang IPersistentMap]
           [com.aerospike.client AerospikeClient Key Bin Operation BatchRead]
           [com.aerospike.client.async EventLoop NioEventLoops EventLoops]
           [com.aerospike.client.cluster Node]
           [com.aerospike.client.policy Policy BatchPolicy ClientPolicy
                                        RecordExistsAction WritePolicy ScanPolicy
                                        InfoPolicy]
           [com.aerospike.client Key Host]
           [aerospike_clj.listeners AsyncExistsListener AsyncDeleteListener AsyncWriteListener
                                    AsyncInfoListener AsyncRecordListener AsyncRecordSequenceListener
                                    AsyncBatchListListener AsyncExistsArrayListener]))

(def EPOCH
  ^{:doc "The 0 date reference for returned record TTL"}
  (.getEpochSecond (Instant/parse "2010-01-01T00:00:00Z")))


(def MAX_BIN_NAME_LENGTH 14)

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
  "Called internally to create the event loops for the client.
  Can also be used to share event loops between several clients."
  [conf]
  (let [elp (policy/map->event-policy conf)]
    (NioEventLoops. elp 1 true "NioEventLoops")))

(defn- client-events-reducer [op-name index op-start-time]
  (fn [op-future client-events]
    (-> op-future
        (p/then (fn [op-result]
                  (pt/on-success client-events op-name op-result index op-start-time)))
        (p/catch (fn [op-exception]
                   (pt/on-failure client-events op-name op-exception index op-start-time))))))

(defn- register-events [op-future client-events op-name index op-start-time]
  (if (empty? client-events)
    op-future
    (reduce (client-events-reducer op-name index op-start-time) op-future client-events)))

(extend-protocol pt/UserKey
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

(defn- ^BatchRead map->batch-read [batch-read-map dbns]
  (let [k ^Key (pt/create-key (:index batch-read-map) dbns (:set batch-read-map))]
    (if (or (= [:all] (:bins batch-read-map))
            (nil? (:bins batch-read-map)))
      (BatchRead. k true)
      (BatchRead. k ^"[Ljava.lang.String;" (utils/v->array String (:bins batch-read-map))))))

;; put
(defn- _put [^AerospikeClient client ^EventLoops event-loops dbns client-events index data policy set-name]
  (let [bins       (data->bins data)
        op-future  (p/deferred)
        start-time (System/nanoTime)]
    (.put client
          ^EventLoop (.next event-loops)
          (AsyncWriteListener. op-future)
          ^WritePolicy policy
          ^Key (pt/create-key index dbns set-name)
          ^"[Lcom.aerospike.client.Bin;" bins)
    (register-events op-future client-events :write index start-time)))

(defrecord SimpleAerospikeClient [^AerospikeClient client
                                  ^EventLoops el
                                  dbns
                                  cluster-name
                                  client-events
                                  close-event-loops?]
  pt/AerospikeReadOps
  (get-single [this index set-name]
    (pt/get-single this index set-name {} [:all]))

  (get-single [this index set-name conf]
    (pt/get-single this index set-name conf [:all]))

  (get-single [_this index set-name conf bin-names]
    (let [op-future  (p/deferred)
          start-time (System/nanoTime)]
      (if (and (= [:all] bin-names)
               (not (utils/single-bin? bin-names)))
        ;; When [:all] is passed as an argument for bin-names and there is more than one bin,
        ;; the `get` method does not require bin-names and the whole record is retrieved
        (.get client
              ^EventLoop (.next el)
              (AsyncRecordListener. op-future)
              ^Policy (:policy conf)
              ^Key (pt/create-key index dbns set-name))
        ;; For all other cases, bin-names are passed to a different `get` method
        (.get ^AerospikeClient client
              ^EventLoop (.next el)
              (AsyncRecordListener. op-future)
              ^Policy (:policy conf)
              ^Key (pt/create-key index dbns set-name)
              ^"[Ljava.lang.String;" (utils/v->array String bin-names)))
      (let [p (p/chain op-future
                       record/record->map
                       (:transcoder conf identity))]
        (register-events p client-events :read index start-time))))

  (get-single-no-meta [this index set-name]
    (pt/get-single this index set-name {:transcoder :payload}))

  (get-single-no-meta [this index set-name bin-names]
    (pt/get-single this index set-name {:transcoder :payload} bin-names))

  (exists? [this index set-name]
    (pt/exists? this index set-name {}))

  (exists? [this index set-name conf]
    (let [op-future  (p/deferred)
          start-time (System/nanoTime)]
      (.exists ^AerospikeClient client
               ^EventLoop (.next el)
               (AsyncExistsListener. op-future)
               ^Policy (:policy conf)
               ^Key (pt/create-key index (:dbns this) set-name))
      (register-events op-future client-events :exists index start-time)))

  pt/AerospikeWriteOps
  (put [this index set-name data expiration]
    (pt/put this index set-name data expiration {}))

  (put [_this index set-name data expiration conf]
    (_put client
          el
          dbns
          client-events
          index
          ((:transcoder conf identity) data)
          (:policy conf (policy/write-policy client expiration))
          set-name))

  (create [this index set-name data expiration]
    (pt/create this index set-name data expiration {}))

  (create [_this index set-name data expiration conf]
    (_put client
          el
          dbns
          client-events
          index
          ((:transcoder conf identity) data)
          (policy/create-only-policy client expiration)
          set-name))

  pt/AerospikeUpdateOps
  (set-single [this index set-name data expiration]
    (pt/set-single this index set-name data expiration {}))

  (set-single [_this index set-name data expiration conf]
    (_put client
          el
          dbns
          client-events
          index
          ((:transcoder conf identity) data)
          (policy/set-policy client expiration)
          set-name))

  (replace-only [this index set-name data expiration]
    (pt/replace-only this index set-name data expiration {}))

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
    (pt/update this index set-name new-record generation new-expiration {}))

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
    (pt/add-bins this index set-name new-data new-expiration {}))

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
              ^EventLoop (.next el)
              (AsyncWriteListener. op-future)
              ^WritePolicy (policy/write-policy client expiration RecordExistsAction/UPDATE_ONLY)
              ^Key (pt/create-key index (:dbns this) set-name))
      (register-events op-future client-events :touch index start-time)))

  pt/AerospikeDeleteOps
  (delete [this index set-name]
    (pt/delete this index set-name {}))

  (delete [this index set-name conf]
    (let [op-future  (p/deferred)
          start-time (System/nanoTime)]
      (.delete client
               ^EventLoop (.next el)
               (AsyncDeleteListener. op-future)
               ^WritePolicy (:policy conf)
               ^Key (pt/create-key index (:dbns this) set-name))
      (register-events op-future client-events :delete index start-time)))

  (delete-bins [this index set-name bin-names new-expiration]
    (pt/delete-bins this index set-name bin-names new-expiration {}))

  (delete-bins [_this index set-name bin-names new-expiration conf]
    (let [bin-names  ((:transcoder conf identity) bin-names)
          policy     (policy/update-only-policy client new-expiration)
          op-future  (p/deferred)
          start-time (System/nanoTime)]
      (.put client
            ^EventLoop (.next el)
            (AsyncWriteListener. op-future)
            ^WritePolicy policy
            ^Key (pt/create-key index dbns set-name)
            ^"[Lcom.aerospike.client.Bin;" (utils/v->array Bin (mapv set-bin-as-null bin-names)))
      (register-events op-future client-events :write index start-time)))

  pt/AerospikeBatchOps
  (get-batch [this batch-reads]
    (pt/get-batch this batch-reads {}))

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
        (register-events d client-events :read-batch nil start-time))))

  (get-multiple [this indices sets]
    (pt/get-multiple this indices sets {}))

  (get-multiple [db indices sets conf]
    (p/all
      (map (fn [[index set-name]] (pt/get-single db index set-name conf))
           (map vector indices sets))))

  (put-multiple [this indices set-names payloads expirations]
    (pt/put-multiple this indices set-names payloads expirations {}))

  (put-multiple [this indices set-names payloads expirations conf]
    (p/all
      (map (fn [[index set-name payload expiration]]
             (pt/put this index set-name payload expiration conf))
           (map vector indices set-names payloads expirations))))

  (exists-batch [this indices]
    (pt/exists-batch this indices {}))

  (exists-batch [this indices conf]
    (let [op-future      (p/deferred)
          start-time     (System/nanoTime)
          aero-namespace (:dbns this)
          indices        (utils/v->array Key (mapv #(pt/create-key (:index %) aero-namespace (:set %)) indices))]
      (.exists client
               ^EventLoop (.next el)
               (AsyncExistsArrayListener. op-future)
               ^BatchPolicy (:policy conf)
               ^"[Lcom.aerospike.client.Key;" indices)
      (let [d (p/chain op-future
                       vec
                       (:transcoder conf identity))]
        (register-events d client-events :exists-batch nil start-time))))

  (operate [this index set-name expiration operations]
    (pt/operate this index set-name expiration operations {}))

  (operate [this index set-name expiration operations conf]
    (if (empty? operations)
      (p/resolved nil)
      (let [op-future  (p/deferred)
            start-time (System/nanoTime)]
        (.operate client
                  ^EventLoop (.next el)
                  (AsyncRecordListener. op-future)
                  ^WritePolicy (:policy conf (policy/write-policy client expiration RecordExistsAction/UPDATE))
                  ^Key (pt/create-key index (:dbns this) set-name)
                  (utils/v->array Operation operations))
        (register-events (p/then op-future record/record->map) client-events "operate" index start-time))))

  (scan-set [_this aero-namespace set-name conf]
    (when-not (fn? (:callback conf))
      (throw (IllegalArgumentException. "(:callback conf) must be a function")))
    (let [op-future  (p/deferred)
          start-time (System/nanoTime)
          bin-names  (:bins conf)]
      (.scanAll client
                ^EventLoop (.next el)
                (AsyncRecordSequenceListener. op-future (:callback conf))
                ^Policy (:policy conf (ScanPolicy.))
                aero-namespace
                set-name
                (when bin-names ^"[Ljava.lang.String;" (utils/v->array String bin-names)))
      (register-events op-future client-events :scan nil start-time)))

  pt/AerospikeAdminOps
  (info [this node info-commands]
    (pt/info this node info-commands {}))

  (info [_this node info-commands conf]
    (let [op-future  (p/deferred)
          start-time (System/nanoTime)]
      (.info client
             ^EventLoop (.next el)
             (AsyncInfoListener. op-future)
             ^InfoPolicy (:policy conf (.infoPolicyDefault ^AerospikeClient client))
             ^Node node
             (into-array String info-commands))
      (register-events op-future client-events :info nil start-time)))

  (get-nodes [_this]
    (into [] (.getNodes client)))

  (get-cluster-stats [_this]
    (-> (.getClusterStats client)
        metrics/construct-cluster-metrics
        metrics/cluster-metrics->dotted)
    #_(let [clients (get-clients selector)]
        (->> clients
             (mapv #(metrics/construct-cluster-metrics (.getClusterStats ^AerospikeClient %)))
             (mapv metrics/cluster-metrics->dotted))))

  (healthy? [this]
    (pt/healthy? this 1000))

  (healthy? [this operation-timeout-ms]
    (let [read-policy (let [p (.readPolicyDefault client)]
                        (set! (.totalTimeout p) operation-timeout-ms)
                        p)
          k           (str "__health__" (rand-int Integer/MAX_VALUE))
          v           (rand-int Integer/MAX_VALUE)
          ttl         (min 1 (int (/ operation-timeout-ms 1000)))
          set-name    "__health-check"]
      (try
        @(pt/put this k set-name v ttl)
        (= v
           @(pt/get-single this k set-name {:transcoder :payload
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
