(ns aerospike-clj.client
  (:refer-clojure :exclude [update])
  (:require [aerospike-clj.utils :as utils]
            [aerospike-clj.metrics :as metrics]
            [manifold.deferred :as d]
            [taoensso.timbre :refer [info error spy]])
  (:import [com.aerospike.client AerospikeClient Host Key Bin Record AerospikeException Operation]
           [com.aerospike.client.async EventLoop EventPolicy NioEventLoops]
           [com.aerospike.client.listener RecordListener WriteListener DeleteListener ExistsListener]
           [com.aerospike.client.policy Policy ClientPolicy WritePolicy RecordExistsAction GenerationPolicy]))

(def EPOCH (.getEpochSecond (java.time.Instant/parse "2010-01-01T00:00:00Z")))
(def MAX_KEY_LENGTH (dec (bit-shift-left 1 13)))

(defprotocol IAerospikeClient
  (get-client [ac index] "Returns the relevant AerospikeClient object for the specific shard")
  (get-client-policy [_] "Returns the ClientPolicy used by the AerospikeClient")
  (get-all-clients [_] "Returns all AerospikeClient objects"))

(defrecord SimpleAerospikeClient [^AerospikeClient ac
                                  ^EventLoop el
                                  ^ClientPolicy cp
                                  ^String dbns
                                  ^String cluster-name
                                  ^boolean logging?
                                  client-events]
  IAerospikeClient
  (get-client ^AerospikeClient [_ _] ac)
  (get-client-policy ^ClientPolicy [_] cp)
  (get-all-clients [_] [ac]))

(defn- create-client-policy [event-loops conf]
  (let [policy (ClientPolicy.)
        {:keys [username password]} conf]
    (when (and username password)
      (set! (.user policy) username)
      (set! (.password policy) password))
    (when (nil? event-loops)
      (throw (ex-info "cannot use nil for event-loops" {:conf conf})))
    (set! (.eventLoops policy) event-loops)
    policy))

(defn- create-client [hosts client-policy]
  (let [hosts-arr (into-array Host (for [h hosts]
                                     ^Host (Host. h 3000)))]
    (AerospikeClient. ^ClientPolicy client-policy ^"[Lcom.aerospike.client.Host;" hosts-arr)))

(defn create-event-loops [conf]
  (let [event-policy (EventPolicy.)
        max-commands-in-process (:max-commands-in-process conf 0)
        max-commands-in-queue (:max-commands-in-queue conf 0)]
    (when (and (pos? max-commands-in-process)
               (zero? max-commands-in-queue))
      (throw (ex-info "setting maxCommandsInProcess>0 and maxCommandsInQueue=0 creates an unbounded delay queue"
                      {:max-commands-in-process max-commands-in-process
                       :max-commands-in-queue max-commands-in-queue})))
    (println (format "event-policy config: max-commands-in-process: %s, max-commands-in-queue: %s"
                     max-commands-in-process max-commands-in-queue))
    (set! (.maxCommandsInProcess event-policy) (int max-commands-in-process))
    (set! (.maxCommandsInQueue event-policy) (int max-commands-in-queue))
    (NioEventLoops. event-policy 1)))

(defn init-simple-aerospike-client
  "hosts should be a seq of known hosts to bootstrap from
  supported config: {username: string password: string event-loops: com.aerospike.client.async.NioEventLoops
  max-commands-in-process: int max-commands-in-queue: int enable-logging: true (default)}"
  ([hosts aero-ns]
   (init-simple-aerospike-client hosts aero-ns {:enable-logging true}))
  ([hosts aero-ns conf]
   (let [cluster-name (utils/cluster-name hosts)
         event-loops (:event-loops conf (create-event-loops conf))
         client-policy (create-client-policy event-loops conf)]
     (println (format ";; Starting aerospike clients for clusters %s with username %s" cluster-name (:username conf)))
     (map->SimpleAerospikeClient {:ac (create-client hosts client-policy)
                                  :el event-loops
                                  :cp client-policy
                                  :dbns aero-ns
                                  :cluster-name cluster-name
                                  :logging? (:enable-logging conf)
                                  :client-events (:client-events conf)}))))

(defn stop-aerospike-clients [db]
  (println ";; Stopping aerospike clients")
  (doseq [^AerospikeClient client (get-all-clients db)]
    (.close client))
  (.close (:el db)))

;; listeners
(defprotocol ClientEvents
  "Continuation functions that are registered when an async DB operation is called.
  The DB passed is IAerospikeClient.
  The value returned from those function will be the value of the returned deferred from the async operation."
  (on-success [_ op-name op-result index op-start-time db]
              "A continuation function. Registered on the operation future and called when operations succeeds.")
  (on-failure [_ op-name op-ex     index op-start-time db]
              "A continuation function. Registered on the operation future and called when operations fails."))

(defn register-events [op-future db op-name index op-start-time]
  (if-let [client-events (:client-events db)]
    (-> op-future
        (d/chain' (fn [op-result]
                    (on-success client-events op-name op-result    index op-start-time db)))
        (d/catch' (fn [op-exception]
                    (on-failure client-events op-name op-exception index op-start-time db))))
    op-future))

(defn- ^ExistsListener reify-exists-listener [op-future]
  (reify ExistsListener
    (^void onFailure [this ^AerospikeException ex]
      (d/error! op-future ex))
    (^void onSuccess [this ^Key k ^boolean exists]
      (d/success! op-future exists))))

(defn ^DeleteListener reify-delete-listener [op-future]
  (reify
    DeleteListener
    (^void onSuccess [this ^Key k ^boolean existed]
      (d/success! op-future existed))
    (^void onFailure [this ^AerospikeException ex]
      (d/error! op-future ex))))

(defn ^WriteListener reify-write-listener [op-future]
  (reify
    WriteListener
    (^void onSuccess [this ^Key _]
      (d/success! op-future true))
    (^void onFailure [this ^AerospikeException ex]
      (d/error! op-future ex))))

(defn- ^RecordListener reify-record-listener [op-future]
  (reify RecordListener
    (^void onFailure [this ^AerospikeException ex]
      (d/error! op-future ex))
    (^void onSuccess [this ^Key k ^Record record]
      (d/success! op-future record))))

(defn- ^Key create-key [^String aero-namespace ^String set-name ^String k]
  (when (< MAX_KEY_LENGTH (.length k))
    (throw (Exception. (format "key is too long: %s..." (subs k 0 40)))))
  (Key. aero-namespace set-name k))

;; get

(defn- record->map [^Record record]
  (and record
       {:payload (get (.bins ^Record record) "")
        :gen ^Integer (.generation ^Record record)
        :ttl ^Integer (.expiration ^Record record)}))

(def ^Policy get-default-read-policy
  (memoize
    (fn [db]
      (let [client (get-client db 1)
            default-read-policy (.getReadPolicyDefault ^AerospikeClient client)]
        (set! (.timeoutDelay default-read-policy) 3000)
        default-read-policy))))

(def ^Policy get-default-write-policy
  (memoize
    (fn [db]
      (let [client (get-client db 1)
            default-write-policy (.getWritePolicyDefault ^AerospikeClient client)]
        (set! (.timeoutDelay default-write-policy) 3000)
        default-write-policy))))

(defn get-single-with-meta
  "returns: (transcoder {:payload record-value, :gen generation, :ttl ttl})."
  ([db index set-name] (get-single-with-meta db index set-name {}))
  ([db index set-name conf]
   (let [client (get-client db index)
         op-future (d/deferred)]
     (.get ^AerospikeClient client
           ^EventLoop (.next ^NioEventLoops (:el db))
           (reify-record-listener op-future)
           ^Policy (:policy conf (get-default-read-policy db))
           (create-key (:dbns db) set-name index))
     (let [d (d/chain' op-future
                       record->map
                       (:transcoder conf identity))]
       (register-events d db "read" index (System/nanoTime))))))

(defn get-multiple-with-meta
  "returns a sequence of maps returned by et-single-with-meta
  with records in corresponding places to the required keys. Indices should be a sequence"
  ([db indices sets]
   (get-multiple-with-meta db indices sets identity))
  ([db indices sets transcoder]
   (apply d/zip'
          (map (fn [[index set-name]] (get-single-with-meta db index set-name {:transcoder transcoder}))
               (map vector indices sets)))))

(defn exists?
  "Asynchronously check if an index exists"
  ([db index set-name] (exists? db index set-name {}))
  ([db index set-name conf]
   (let [client (get-client db index)
         op-future (d/deferred)]
     (.exists ^AerospikeClient client
              ^EventLoop (.next ^NioEventLoops (:el db))
              (reify-exists-listener op-future)
              ^Policy (:policy conf (get-default-read-policy db))
              (create-key (:dbns db) set-name index))
     (register-events op-future db "exists" index (System/nanoTime)))))

(defn get-single
  "returns record-value only."
  [db index set-name]
  (get-single-with-meta db index set-name {:transcoder :payload}))

;; put

(defn ^WritePolicy write-policy [expiry record-exists-action]
  (let [wp (WritePolicy.)]
    (set! (.timeoutDelay wp) 3000)
    (set! (.expiration wp) expiry)
    (set! (.recordExistsAction wp) (case record-exists-action
                                     :replace RecordExistsAction/REPLACE
                                     :create-only RecordExistsAction/CREATE_ONLY
                                     :update-only RecordExistsAction/UPDATE_ONLY
                                     :update RecordExistsAction/UPDATE))
    wp))

(defn- update-policy [generation new-expiry]
  (let [wp (write-policy new-expiry :replace)]
    (set! (.generation wp) generation)
    (set! (.generationPolicy wp) GenerationPolicy/EXPECT_GEN_EQUAL)
    wp))

(defn- _put [db index data policy set-name]
  (let [client (get-client db index)
        bins (into-array Bin [^Bin (Bin. "" data)])
        op-future (d/deferred)]
    (.put ^AerospikeClient client
          ^EventLoop (.next ^NioEventLoops (:el db))
          ^WriteListener (reify-write-listener op-future)
          ^WritePolicy policy
          (create-key (:dbns db) set-name index)
          ^"[Lcom.aerospike.client.Bin;" bins)
    (register-events op-future db "write" index (System/nanoTime))))

(defn put
  "Writes <data> into a record with the key <index>, with the ttl of <expiry> seconds.
  Data should be string.
  "
  ([db index set-name data expiry] (put db index set-name data expiry {}))
  ([db index set-name data expiry conf]
   (_put db
         index
         ((:transcoder conf identity) data)
         (write-policy expiry (:record-exists-action conf :replace))
         set-name)))

(defn create [db index set-name data expiry]
  (put db index set-name data expiry {:record-exists-action :create-only}))


(defn update
  "Writing a new value for the key <index>.
  Generation: the expected modification count of the record (i.e. how many times was it modified before my current action).  "
  ([db index set-name new-record generation new-expiry]
   (update db index set-name new-record generation new-expiry {}))
  ([db index set-name new-record generation new-expiry conf]
   (_put db
         index
         ((:transcoder conf identity) new-record)
         (update-policy generation new-expiry)
         set-name)))

(defn touch
  "updates the ttl of the record stored under the key of <index> to <expiry> seconds from now."
  [db index set-name expiry]
  (let [client (get-client db index)
        op-future (d/deferred)]
    (.touch ^AerospikeClient client
            ^EventLoop (.next ^NioEventLoops (:el db))
            ^WriteListener (reify-write-listener op-future)
            ^WritePolicy (write-policy expiry :update-only)
            (create-key (:dbns db) set-name index))
    (register-events op-future db "touch" index (System/nanoTime))))

;; delete

(defn delete
  "deletes the record stored for key <index>."
  [db index set-name]
  (let [client (get-client db index)
        op-future (d/deferred)]
    (.delete ^AerospikeClient client
             ^EventLoop (.next ^NioEventLoops (:el db))
             ^DeleteListener (reify-delete-listener op-future)
             ^WritePolicy (.getWritePolicyDefault ^AerospikeClient client)
             (create-key (:dbns db) set-name index))
    (register-events op-future db "delete" index (System/nanoTime))))

;; operate

(defn operate
  "Asynchronously perform multiple read/write operations on a single key in one batch call.
  This method registers the command with an event loop and returns. The event loop thread
  will process the command and send the results to the listener."
  [db index set-name expiry record-exists-action & operations]
  (if-not (seq operations)
    (d/success-deferred nil)
    (let [client (get-client db index)
          op-future (d/deferred)]
      (.operate ^AerospikeClient client
                ^EventLoop (.next^NioEventLoops (:el db))
                ^RecordListener (reify-record-listener op-future)
                ^WritePolicy (write-policy expiry record-exists-action)
                (create-key (:dbns db) set-name index)
                (into-array Operation operations))
      (register-events (d/chain' op-future record->map) db "operate" index (System/nanoTime)))))

;; metrics
(defn get-cluster-stats
  "For each xdr client, returns a vector of [metric-name metric-val] 2-tuples.
  The metric name is a dot separated string that should be convenient for
  reporting to statsd/graphite. All values are gauges."
  [db]
  (let [clients (get-all-clients db)]
    (->> clients
         (mapv #(metrics/construct-cluster-metrics (.getClusterStats ^AerospikeClient %)))
         (mapv metrics/cluster-metrics->dotted))))

;; health

(defn healty?
  "Returns true iff the cluster is reachable and can take reads and writes. Uses __health-check set to avoid data collisions. `operation-timeout-ms` is for total timeout of reads (including 2 retries so an small over estimation is advised to avoid false negatives."
  [db operation-timeout-ms]
  (let [read-policy (let [p (Policy.)]
                      (set! (.totalTimeout p) operation-timeout-ms)
                      p)
        k (str "__health__" (rand-int 1000))
        v 1
        ttl (min 1 (int (/ operation-timeout-ms 1000)))
        set-name "__health-check"]
    (try
      @(create db k set-name v ttl)
      (= v
         @(get-single-with-meta db k set-name {:transcoder :payload
                                               :policy read-policy}))
      (catch Exception ex
        false))))

;; etc

(defn expiry-unix
  "Returns the epoch time of now + <ttl> seconds"
  [ttl]
  (+ ttl EPOCH))
