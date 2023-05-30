(ns aerospike-clj.client
  (:refer-clojure :exclude [update])
  (:require [clojure.string :as s]
            [clojure.tools.logging :as log]
            [promesa.core :as p]
            [aerospike-clj.policy :as policy]
            [aerospike-clj.bins :as bins]
            [aerospike-clj.utils :as utils]
            [aerospike-clj.metrics :as metrics]
            [aerospike-clj.key :as as-key]
            [aerospike-clj.listeners]
            [aerospike-clj.aerospike-record :as record]
            [aerospike-clj.protocols :as pt])
  (:import [java.time Instant]
           [java.util List Collection ArrayList Arrays]
           [com.aerospike.client AerospikeClient Key Bin Operation BatchRead]
           [com.aerospike.client.async EventLoop NioEventLoops EventLoops]
           [com.aerospike.client.cluster Node]
           [com.aerospike.client.policy Policy BatchPolicy ClientPolicy
                                        RecordExistsAction WritePolicy ScanPolicy
                                        InfoPolicy]
           [com.aerospike.client Key Host BatchRecord]
           [aerospike_clj.listeners AsyncExistsListener AsyncDeleteListener AsyncWriteListener
                                    AsyncInfoListener AsyncRecordListener AsyncRecordSequenceListener
                                    AsyncBatchListListener AsyncExistsArrayListener AsyncBatchOperateListListener]
           (com.aerospike.client.listener BatchOperateListListener)))

(def
  ^{:doc   "The 0 date reference for returned record TTL"
    :const true}
  EPOCH
  (.getEpochSecond (Instant/parse "2010-01-01T00:00:00Z")))

(defn- create-client
  "Returns the Java `AerospikeClient` instance.

  Expects:
    :hosts - a seq of host strings, can include default-port or not.
             Format: [hostname1[:tlsname1][:port1],...]
             Example: [\"aerospike-001\" \"aerospike-002\"]
             Example: [\"aerospike-001:3000\" \"aerospike-002:3010\"]

    :client-policy - a ClientPolicy instance

    :default-port - the default-port to use in case there's no port specified in the host names."
  [hosts client-policy default-port]
  (let [hosts-str (s/join "," hosts)
        hosts-arr (Host/parseHosts hosts-str default-port)]
    (AerospikeClient. ^ClientPolicy client-policy ^"[Lcom.aerospike.client.Host;" hosts-arr)))

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

(defn- batch-record->map [^BatchRecord batch-record]
  (let [k (.key batch-record)]
    (-> (record/record->map (.record batch-record))
        (assoc :index (.toString (.userKey k)))
        (assoc :set (.setName k))
        (assoc :result-code (.resultCode batch-record)))))

(defn- map->batch-read ^BatchRead [batch-read-map dbns]
  (let [k ^Key (pt/create-key (:index batch-read-map) dbns (:set batch-read-map))]
    (if (or (= [:all] (:bins batch-read-map))
            (nil? (:bins batch-read-map)))
      (BatchRead. k true)
      (BatchRead. k ^"[Ljava.lang.String;" (utils/v->array String (:bins batch-read-map))))))

;; put
(defn- put* [^AerospikeClient client ^EventLoops event-loops dbns client-events index data policy set-name]
  (let [bins       (bins/data->bins data)
        op-future  (p/deferred)
        start-time (System/nanoTime)]
    (.put client
          ^EventLoop (.next ^EventLoops event-loops)
          (AsyncWriteListener. op-future)
          ^WritePolicy policy
          ^Key (pt/create-key index dbns set-name)
          ^"[Lcom.aerospike.client.Bin;" bins)
    (register-events op-future client-events :write index start-time)))

(deftype SimpleAerospikeClient [client
                                el
                                hosts
                                dbns
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
        (.get ^AerospikeClient client
              ^EventLoop (.next ^EventLoops el)
              (AsyncRecordListener. op-future)
              ^Policy (:policy conf)
              ^Key (pt/create-key index dbns set-name))
        ;; For all other cases, bin-names are passed to a different `get` method
        (.get ^AerospikeClient client
              ^EventLoop (.next ^EventLoops el)
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

  (exists? [_this index set-name conf]
    (let [op-future  (p/deferred)
          start-time (System/nanoTime)]
      (.exists ^AerospikeClient client
               ^EventLoop (.next ^EventLoops el)
               (AsyncExistsListener. op-future)
               ^Policy (:policy conf)
               ^Key (pt/create-key index dbns set-name))
      (register-events op-future client-events :exists index start-time)))

  (get-batch [this batch-reads]
    (pt/get-batch this batch-reads {}))

  (get-batch [_this batch-reads conf]
    (let [op-future       (p/deferred)
          start-time      (System/nanoTime)
          batch-reads-arr (ArrayList. ^Collection (mapv #(map->batch-read % dbns) batch-reads))]
      (.get ^AerospikeClient client
            ^EventLoop (.next ^EventLoops el)
            (AsyncBatchListListener. op-future)
            ^BatchPolicy (:policy conf)
            ^List batch-reads-arr)
      (let [d (p/chain op-future
                       #(mapv batch-record->map %)
                       (:transcoder conf identity))]
        (register-events d client-events :read-batch nil start-time))))

  (exists-batch [this indices]
    (pt/exists-batch this indices {}))

  (exists-batch [_this indices conf]
    (let [op-future  (p/deferred)
          start-time (System/nanoTime)
          indices    (utils/v->array Key (mapv #(pt/create-key (:index %) dbns (:set %)) indices))]
      (.exists ^AerospikeClient client
               ^EventLoop (.next ^EventLoops el)
               (AsyncExistsArrayListener. op-future)
               ^BatchPolicy (:policy conf)
               ^"[Lcom.aerospike.client.Key;" indices)
      (let [d (p/chain op-future
                       vec
                       (:transcoder conf identity))]
        (register-events d client-events :exists-batch nil start-time))))

  pt/AerospikeWriteOps
  (put [this index set-name data expiration]
    (pt/put this index set-name data expiration {}))

  (put [_this index set-name data expiration conf]
    (put* client
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
    (put* client
          el
          dbns
          client-events
          index
          ((:transcoder conf identity) data)
          (policy/create-only-policy client expiration)
          set-name))

  (put-multiple [this indices set-names payloads expirations]
    (pt/put-multiple this indices set-names payloads expirations {}))

  (put-multiple [this indices set-names payloads expirations conf]
    (p/all
      (map (fn [[index set-name payload expiration]]
             (pt/put this index set-name payload expiration conf))
           (map vector indices set-names payloads expirations))))

  pt/AerospikeUpdateOps
  (set-single [this index set-name data expiration]
    (pt/set-single this index set-name data expiration {}))

  (set-single [_this index set-name data expiration conf]
    (put* client
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
    (put* client
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
    (put* client
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
    (put* client
          el
          dbns
          client-events
          index
          ((:transcoder conf identity) new-data)
          (policy/update-only-policy client new-expiration)
          set-name))

  (touch [_this index set-name expiration]
    (let [op-future  (p/deferred)
          start-time (System/nanoTime)]
      (.touch ^AerospikeClient client
              ^EventLoop (.next ^EventLoops el)
              (AsyncWriteListener. op-future)
              ^WritePolicy (policy/write-policy client expiration RecordExistsAction/UPDATE_ONLY)
              ^Key (pt/create-key index dbns set-name))
      (register-events op-future client-events :touch index start-time)))

  pt/AerospikeDeleteOps
  (delete [this index set-name]
    (pt/delete this index set-name {}))

  (delete [_this index set-name conf]
    (let [op-future  (p/deferred)
          start-time (System/nanoTime)]
      (.delete ^AerospikeClient client
               ^EventLoop (.next ^EventLoops el)
               (AsyncDeleteListener. op-future)
               ^WritePolicy (:policy conf)
               ^Key (pt/create-key index dbns set-name))
      (register-events op-future client-events :delete index start-time)))

  (delete-bins [this index set-name bin-names new-expiration]
    (pt/delete-bins this index set-name bin-names new-expiration {}))

  (delete-bins [_this index set-name bin-names new-expiration conf]
    (let [bin-names  ((:transcoder conf identity) bin-names)
          policy     (policy/update-only-policy client new-expiration)
          op-future  (p/deferred)
          start-time (System/nanoTime)]
      (.put ^AerospikeClient client
            ^EventLoop (.next ^EventLoops el)
            (AsyncWriteListener. op-future)
            ^WritePolicy policy
            ^Key (pt/create-key index dbns set-name)
            ^"[Lcom.aerospike.client.Bin;" (utils/v->array Bin (mapv bins/set-bin-as-null bin-names)))
      (register-events op-future client-events :write index start-time)))

  pt/AerospikeSingleIndexBatchOps
  (operate [this index set-name expiration operations]
    (pt/operate this index set-name expiration operations {}))

  (operate [_this index set-name expiration operations conf]
    (if (empty? operations)
      (p/resolved nil)
      (let [op-future  (p/deferred)
            start-time (System/nanoTime)]
        (.operate ^AerospikeClient client
                  ^EventLoop (.next ^EventLoops el)
                  (AsyncRecordListener. op-future)
                  ^WritePolicy (:policy conf (policy/write-policy client expiration RecordExistsAction/UPDATE))
                  ^Key (pt/create-key index dbns set-name)
                  (utils/v->array Operation operations))
        (register-events (p/then op-future record/record->map) client-events :operate index start-time))))

  pt/AerospikeBatchOps
  (batch-operate [this batch-records]
    (pt/batch-operate this batch-records {}))

  (batch-operate [_this batch-records conf]
    (let [op-future  (p/deferred)
          policy     (or (:policy conf) (policy/map->batch-policy conf))
          batch-list (if (list? batch-records)
                       batch-records
                       (->> batch-records
                            (utils/v->array BatchRecord)
                            (Arrays/asList)))
          start-time (System/nanoTime)]
      (.operate ^AerospikeClient client
                ^EventLoop (.next ^EventLoops el)
                ^BatchOperateListListener (AsyncBatchOperateListListener. op-future)
                ^BatchPolicy policy
                ^List batch-list)
      (let [d (p/chain op-future
                       #(mapv batch-record->map %)
                       (:transcoder conf identity))]
        (register-events d client-events :batch-operate nil start-time))))


  pt/AerospikeSetOps
  (scan-set [_this aero-namespace set-name conf]
    (when-not (fn? (:callback conf))
      (throw (IllegalArgumentException. "(:callback conf) must be a function")))
    (let [op-future  (p/deferred)
          start-time (System/nanoTime)
          bin-names  (:bins conf)]
      (.scanAll ^AerospikeClient client
                ^EventLoop (.next ^EventLoops el)
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
      (.info ^AerospikeClient client
             ^EventLoop (.next ^EventLoops el)
             (AsyncInfoListener. op-future)
             ^InfoPolicy (:policy conf (.infoPolicyDefault ^AerospikeClient client))
             ^Node node
             (into-array String info-commands))
      (register-events op-future client-events :info nil start-time)))

  (get-nodes [_this]
    (into [] (.getNodes ^AerospikeClient client)))

  (get-cluster-stats [_this]
    (-> (.getClusterStats ^AerospikeClient client)
        metrics/construct-cluster-metrics
        metrics/cluster-metrics->dotted))

  (healthy? [this]
    (pt/healthy? this 1000))

  (healthy? [this operation-timeout-ms]
    (let [read-policy (let [p ^Policy (.readPolicyDefault ^AerospikeClient client)]
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

  (stop [_this]
    (log/info "Stopping aerospike client for hosts" hosts)
    (.close ^AerospikeClient client)
    (when close-event-loops?
      (.close ^EventLoops el))))

(defn expiry-unix
  "Converts an Aerospike TTL (in seconds) relative to \"2010-01-01T00:00:00Z\"
  into a TTL (in seconds) relative to the UNIX epoch."
  [ttl]
  (+ ttl EPOCH))

(defn init-simple-aerospike-client
  "`hosts` should be a seq of known hosts to bootstrap from. These can include a
  desired port, as in the form `host:port`. These would take precedence over `:port`
  as defined below.

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
  - :port - to specify a single port to use for all host names, only if ports aren't
  explicit in the host names set above in `:hosts`. In case that a port isn't explicitly
  stated in the `hosts`, and the `:port` parameter is missing, port 3000 is used
  by default.
  - :client-events - an implementation of ClientEvents. Either a single one or a vector
    thereof. In the case of a vector, the client will chain the instances by order."
  ([hosts aero-ns]
   (init-simple-aerospike-client hosts aero-ns {}))
  ([hosts aero-ns conf]
   (let [close-event-loops? (nil? (:event-loops conf))
         event-loops        (or (:event-loops conf) (create-event-loops conf))
         client-policy      (:client-policy conf (policy/create-client-policy event-loops conf))]
     (log/info (format "Starting aerospike client for hosts %s with username %s" hosts (get conf "username")))
     (->SimpleAerospikeClient (create-client hosts client-policy (:port conf 3000))
                              event-loops
                              hosts
                              aero-ns
                              (utils/vectorize (:client-events conf))
                              close-event-loops?))))
