(ns aerospike-clj.client
  (:refer-clojure :exclude [update])
  (:require [aerospike-clj.bins :as bins]
            [aerospike-clj.functions :as functions]
            [aerospike-clj.key :as as-key]
            [aerospike-clj.listeners]
            [aerospike-clj.metrics :as metrics]
            [aerospike-clj.policy :as policy]
            [aerospike-clj.protocols :as pt]
            [aerospike-clj.utils :as utils]
            [clojure.string :as s]
            [clojure.tools.logging :as log])
  (:import (aerospike_clj.listeners AsyncBatchListListener AsyncBatchOperateListListener AsyncDeleteListener
                                    AsyncExistsArrayListener AsyncExistsListener AsyncInfoListener
                                    AsyncRecordListener AsyncRecordSequenceListener AsyncWriteListener)
           (com.aerospike.client BatchRecord Host Key)
           (com.aerospike.client AerospikeClient BatchRead Bin Key Operation)
           (com.aerospike.client.async EventLoop EventLoops NioEventLoops)
           (com.aerospike.client.cluster Node)
           (com.aerospike.client.listener BatchOperateListListener)
           (com.aerospike.client.policy BatchPolicy ClientPolicy InfoPolicy
                                        Policy RecordExistsAction ScanPolicy
                                        WritePolicy)
           (java.time Instant)
           (java.util ArrayList Arrays Collection List)
           (java.util.concurrent CompletableFuture Executor ForkJoinPool)
           (java.util.function Consumer Function)))

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

(extend-protocol pt/UserKey
  Key
  (create-key ^Key [this _ _]
    this)
  Object
  (create-key ^Key [this as-namespace set-name]
    (as-key/create-key this as-namespace set-name)))

(defn- map->batch-read ^BatchRead [batch-read-map dbns]
  (let [k ^Key (pt/create-key (:index batch-read-map) dbns (:set batch-read-map))]
    (if (or (= [:all] (:bins batch-read-map))
            (nil? (:bins batch-read-map)))
      (BatchRead. k true)
      (BatchRead. k ^"[Ljava.lang.String;" (utils/v->array String (:bins batch-read-map))))))

;; put
(defn- put* ^CompletableFuture [^AerospikeClient client ^EventLoops event-loops dbns index data policy set-name]
  (let [bins      (bins/data->bins data)
        op-future (CompletableFuture.)]
    (.put client
          ^EventLoop (.next ^EventLoops event-loops)
          (AsyncWriteListener. op-future)
          ^WritePolicy policy
          ^Key (pt/create-key index dbns set-name)
          ^"[Lcom.aerospike.client.Bin;" bins)
    op-future))

(deftype SimpleAerospikeClient [client
                                el
                                ^Executor completion-executor
                                hosts
                                dbns
                                close-event-loops?]
  pt/AerospikeReadOps
  (get-single [this index set-name]
    (pt/get-single this index set-name {} [:all]))

  (get-single [this index set-name conf]
    (pt/get-single this index set-name conf [:all]))

  (get-single [_this index set-name conf bin-names]
    (let [op-future (CompletableFuture.)]
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
      (-> ^CompletableFuture op-future
          (.thenApplyAsync functions/record->map-function completion-executor))))

  (get-single-no-meta [this index set-name]
    (-> ^CompletableFuture (pt/get-single this index set-name)
        (.thenApply functions/extract-payload-function)))

  (get-single-no-meta [this index set-name bin-names]
    (-> ^CompletableFuture (pt/get-single this index set-name {} bin-names)
        (.thenApply functions/extract-payload-function)))

  (exists? [this index set-name]
    (pt/exists? this index set-name {}))

  (exists? [_this index set-name conf]
    (let [op-future (CompletableFuture.)]
      (.exists ^AerospikeClient client
               ^EventLoop (.next ^EventLoops el)
               (AsyncExistsListener. op-future)
               ^Policy (:policy conf)
               ^Key (pt/create-key index dbns set-name))
      (-> ^CompletableFuture op-future
          (.thenApplyAsync functions/identity-function completion-executor))))

  (get-batch [this batch-reads]
    (pt/get-batch this batch-reads {}))

  (get-batch [_this batch-reads conf]
    (let [op-future       (CompletableFuture.)
          batch-reads-arr (ArrayList. ^Collection (mapv #(map->batch-read % dbns) batch-reads))]
      (.get ^AerospikeClient client
            ^EventLoop (.next ^EventLoops el)
            (AsyncBatchListListener. op-future)
            ^BatchPolicy (:policy conf)
            ^List batch-reads-arr)
      (-> ^CompletableFuture op-future
          (.thenApplyAsync functions/mapv-batch-record->map-function completion-executor))))

  (exists-batch [this indices]
    (pt/exists-batch this indices {}))

  (exists-batch [_this indices conf]
    (let [op-future (CompletableFuture.)
          indices   (utils/v->array Key (mapv #(pt/create-key (:index %) dbns (:set %)) indices))]
      (.exists ^AerospikeClient client
               ^EventLoop (.next ^EventLoops el)
               (AsyncExistsArrayListener. op-future)
               ^BatchPolicy (:policy conf)
               ^"[Lcom.aerospike.client.Key;" indices)
      (-> ^CompletableFuture op-future
          (.thenApplyAsync functions/vec-function completion-executor))))

  pt/AerospikeWriteOps
  (put [this index set-name data expiration]
    (pt/put this index set-name data expiration {}))

  (put [_this index set-name data expiration conf]
    (put* client
          el
          dbns
          index
          data
          (:policy conf (policy/write-policy client expiration))
          set-name))

  (create [this index set-name data expiration]
    (pt/create this index set-name data expiration {}))

  (create [_this index set-name data expiration _conf]
    (put* client
          el
          dbns
          index
          data
          (policy/create-only-policy client expiration)
          set-name))

  (put-multiple [this indices set-names payloads expirations]
    (pt/put-multiple this indices set-names payloads expirations {}))

  (put-multiple [this indices set-names payloads expirations conf]
    (let [futures (mapv (fn [[index set-name payload expiration]]
                          (pt/put this index set-name payload expiration conf))
                        (map vector indices set-names payloads expirations))]
      (-> (CompletableFuture/allOf (into-array futures))
          (.thenApply (reify Function
                        (apply [_ _]
                          ; Now that we've waited for all the futures to complete, we can collect the results
                          (let [results (ArrayList. (count futures))]
                            (.forEach ^Collection futures
                                      (reify Consumer
                                        (accept [_ future]
                                          (.add results (.getNow ^CompletableFuture future nil)))))
                            (into [] results))))))))

  pt/AerospikeUpdateOps
  (set-single [this index set-name data expiration]
    (pt/set-single this index set-name data expiration {}))

  (set-single [_this index set-name data expiration _conf]
    (put* client
          el
          dbns
          index
          data
          (policy/set-policy client expiration)
          set-name))

  (replace-only [this index set-name data expiration]
    (pt/replace-only this index set-name data expiration {}))

  (replace-only [_this index set-name data expiration _conf]
    (put* client
          el
          dbns
          index
          data
          (policy/replace-only-policy client expiration)
          set-name))

  (update [this index set-name new-record generation new-expiration]
    (pt/update this index set-name new-record generation new-expiration {}))

  (update [_this index set-name new-record generation new-expiration _conf]
    (put* client
          el
          dbns
          index
          new-record
          (policy/update-policy client generation new-expiration)
          set-name))

  (add-bins [this index set-name new-data new-expiration]
    (pt/add-bins this index set-name new-data new-expiration {}))

  (add-bins [_this index set-name new-data new-expiration _conf]
    (put* client
          el
          dbns
          index
          new-data
          (policy/update-only-policy client new-expiration)
          set-name))

  (touch [this index set-name expiration]
    (pt/touch this index set-name expiration {}))

  (touch [_this index set-name expiration _conf]
    (let [op-future (CompletableFuture.)]
      (.touch ^AerospikeClient client
              ^EventLoop (.next ^EventLoops el)
              (AsyncWriteListener. op-future)
              ^WritePolicy (policy/write-policy client expiration RecordExistsAction/UPDATE_ONLY)
              ^Key (pt/create-key index dbns set-name))
      (-> ^CompletableFuture op-future
          (.thenApplyAsync functions/identity-function completion-executor))))

  pt/AerospikeDeleteOps
  (delete [this index set-name]
    (pt/delete this index set-name {}))

  (delete [_this index set-name conf]
    (let [op-future (CompletableFuture.)]
      (.delete ^AerospikeClient client
               ^EventLoop (.next ^EventLoops el)
               (AsyncDeleteListener. op-future)
               ^WritePolicy (:policy conf)
               ^Key (pt/create-key index dbns set-name))
      (-> ^CompletableFuture op-future
          (.thenApplyAsync functions/identity-function completion-executor))))

  (delete-bins [this index set-name bin-names new-expiration]
    (pt/delete-bins this index set-name bin-names new-expiration {}))

  (delete-bins [_this index set-name bin-names new-expiration _conf]
    (let [policy    (policy/update-only-policy client new-expiration)
          op-future (CompletableFuture.)]
      (.put ^AerospikeClient client
            ^EventLoop (.next ^EventLoops el)
            (AsyncWriteListener. op-future)
            ^WritePolicy policy
            ^Key (pt/create-key index dbns set-name)
            ^"[Lcom.aerospike.client.Bin;" (utils/v->array Bin (mapv bins/set-bin-as-null bin-names)))
      (-> ^CompletableFuture op-future
          (.thenApplyAsync functions/identity-function completion-executor))))

  pt/AerospikeSingleIndexBatchOps
  (operate [this index set-name expiration operations]
    (pt/operate this index set-name expiration operations {}))

  (operate [_this index set-name expiration operations conf]
    (if (empty? operations)
      (CompletableFuture/completedFuture nil)
      (let [op-future (CompletableFuture.)]
        (.operate ^AerospikeClient client
                  ^EventLoop (.next ^EventLoops el)
                  (AsyncRecordListener. op-future)
                  ^WritePolicy (:policy conf (policy/write-policy client expiration RecordExistsAction/UPDATE))
                  ^Key (pt/create-key index dbns set-name)
                  (utils/v->array Operation operations))
        (-> ^CompletableFuture op-future
            (.thenApplyAsync functions/record->map-function completion-executor)))))

  pt/AerospikeBatchOps
  (batch-operate [this batch-records]
    (pt/batch-operate this batch-records {}))

  (batch-operate [_this batch-records conf]
    (let [op-future  (CompletableFuture.)
          policy     (:policy conf)
          batch-list (if (list? batch-records)
                       batch-records
                       (->> batch-records
                            (utils/v->array BatchRecord)
                            (Arrays/asList)))]
      (.operate ^AerospikeClient client
                ^EventLoop (.next ^EventLoops el)
                ^BatchOperateListListener (AsyncBatchOperateListListener. op-future)
                ^BatchPolicy policy
                ^List batch-list)
      (-> ^CompletableFuture op-future
          (.thenApplyAsync functions/mapv-batch-record->map-function completion-executor))))


  pt/AerospikeSetOps
  (scan-set [_this aero-namespace set-name conf]
    (when-not (fn? (:callback conf))
      (throw (IllegalArgumentException. "(:callback conf) must be a function")))
    (let [op-future (CompletableFuture.)
          bin-names (:bins conf)]
      (.scanAll ^AerospikeClient client
                ^EventLoop (.next ^EventLoops el)
                (AsyncRecordSequenceListener. op-future (:callback conf))
                ^Policy (:policy conf (ScanPolicy.))
                aero-namespace
                set-name
                (when bin-names ^"[Ljava.lang.String;" (utils/v->array String bin-names)))
      (-> ^CompletableFuture op-future
          (.thenApplyAsync functions/identity-function completion-executor))))

  pt/AerospikeAdminOps
  (info [this node info-commands]
    (pt/info this node info-commands {}))

  (info [_this node info-commands conf]
    (let [op-future (CompletableFuture.)]
      (.info ^AerospikeClient client
             ^EventLoop (.next ^EventLoops el)
             (AsyncInfoListener. op-future)
             ^InfoPolicy (:policy conf (.infoPolicyDefault ^AerospikeClient client))
             ^Node node
             (into-array String info-commands))
      (-> ^CompletableFuture op-future
          (.thenApplyAsync functions/identity-function completion-executor))))

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
           (-> ^CompletableFuture (pt/get-single this k set-name {:policy read-policy})
               (.thenApply functions/extract-payload-function)
               deref))
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
  by default."
  ([hosts aero-ns]
   (init-simple-aerospike-client hosts aero-ns {}))
  ([hosts aero-ns conf]
   (let [close-event-loops?  (nil? (:event-loops conf))
         event-loops         (or (:event-loops conf) (create-event-loops conf))
         completion-executor (:completion-executor conf (ForkJoinPool/commonPool))
         client-policy       (:client-policy conf (policy/create-client-policy event-loops conf))]
     (log/info (format "Starting aerospike client for hosts %s with username %s" hosts (get conf "username")))
     (->SimpleAerospikeClient (create-client hosts client-policy (:port conf 3000))
                              event-loops
                              completion-executor
                              hosts
                              aero-ns
                              close-event-loops?))))
