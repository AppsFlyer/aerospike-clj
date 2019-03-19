(ns aerospike-clj.policy
  (:import [com.aerospike.client AerospikeClient]
           [com.aerospike.client.async EventPolicy]
           [com.aerospike.client.policy Policy ClientPolicy WritePolicy RecordExistsAction GenerationPolicy CommitLevel
                                        AuthMode ConsistencyLevel Priority Replica]))

(defmacro set-java [obj conf obj-name]
  `(when (get ~conf ~obj-name)
     (set! (. ~obj ~(symbol obj-name)) (get ~conf ~obj-name))))

(defn lowercase-first [s]
  (apply str (Character/toLowerCase ^Character (first s)) (rest s)))

(defmacro set-java-enum [obj conf obj-name]
  `(when (get ~conf ~obj-name)
     (set! (. ~obj ~(symbol (lowercase-first obj-name)))
           (Enum/valueOf ~(symbol obj-name) (get ~conf ~obj-name)))))

(defn ^Policy map->policy
  "Create a (read) `Policy` from a map. Enumeration names should start with capitalized letter.
  This function is slow due to possible reflection."
  [conf]
  (let [p (Policy.)
        conf (merge {"timeoutDelay" 3000} conf)]
      (set-java-enum p conf "ConsistencyLevel")
      (set-java p conf "linearizeRead")
      (set-java p conf "maxRetries")
      (set-java-enum p conf "Priority")
      (set-java-enum p conf "Replica")
      (set-java p conf "sendKey")
      (set-java p conf "sleepBetweenRetries")
      (set-java p conf "socketTimeout")
      (set-java p conf "timeoutDelay")
      (set-java p conf "totalTimeout")
      p))

(defn ^WritePolicy map->write-policy
  "Create a `WritePolicy` from a map. Keys are strings identical to field names
  enum fields should start capitalized. This function is slow and involves reflection.
  For a faster creation use `write-policy` which uses the client policy caching."
  [conf]
  (let [wp (WritePolicy. (map->policy conf))]
    (set-java-enum wp conf "CommitLevel")
    (set-java wp conf "durableDelete")
    (set-java wp conf "expiration")
    (set-java wp conf "generation")
    (set-java-enum wp conf "GenerationPolicy")
    (set-java-enum wp conf "RecordExistsAction")
    (set-java wp conf "respondAllOps")
    wp))

(defn ^WritePolicy write-policy
  "Create a write policy to be passed to put methods via `{:policy wp}`.
  Also used in `update` and `create`"
  ([client expiration]
   (write-policy client expiration (RecordExistsAction/REPLACE)));; TODO document this
  ([client expiration record-exists-action]
   (let [wp (WritePolicy. (.getWritePolicyDefault ^AerospikeClient client))]
     (set! (.expiration wp) expiration)
     (set! (.recordExistsAction wp) record-exists-action)
     wp)))

(defn ^WritePolicy update-policy
  "Create a write policy with `expiration`, expected `generation`
  and EXPECT_GEN_EQUAL generation policy."
  [client generation new-expiration]
  (let [wp (write-policy client new-expiration)]
    (set! (.generation wp) generation)
    (set! (.generationPolicy wp) GenerationPolicy/EXPECT_GEN_EQUAL)
    wp))

(defn ^WritePolicy create-only-policy
  "Create a write policy with CREATE_ONLY record exists action."
  [client expiration]
  (let [wp (write-policy client expiration)]
    (set! (.recordExistsAction wp) RecordExistsAction/CREATE_ONLY)
    wp))

(defn ^WritePolicy replace-only-policy
  "Create a write policy with REPLACE_ONLY record exists action. Fails if the record does not exists"
  [client expiration]
  (let [wp (write-policy client expiration)]
    (set! (.recordExistsAction wp) RecordExistsAction/REPLACE_ONLY)
    wp))

(defn ^WritePolicy update-bins-policy
  "Create a write policy with UPDATE_ONLY record exists action. The policy helps add/delete bins in
  records without replacing existing data."
  [client new-expiration]
  (let [wp (write-policy client new-expiration)]
    (set! (.recordExistsAction wp) RecordExistsAction/UPDATE_ONLY)
    wp))

(defn ^EventPolicy map->event-policy
  "Create an `EventPolicy` from a map. Usage same as `map->write-policy`."
  [conf]
  (let [event-policy (EventPolicy.)]
    (when (and (pos? (get conf "maxCommandsInProcess" 0))
               (zero? (get conf "maxCommandsInQueue" 0)))
      (throw (ex-info "setting maxCommandsInProcess>0 and maxCommandsInQueue=0 creates an unbounded delay queue"
                      {:conf conf})))
    (set! (.maxCommandsInProcess event-policy) (get conf "maxCommandsInProcess" 0))
    (set! (.maxCommandsInQueue event-policy) (get conf "maxCommandsInQueue" 0))
    (let [ep (EventPolicy.)]
      (set-java ep conf "maxCommandsInProcess")
      (set-java ep conf "maxCommandsInQueue")
      (set-java ep conf "commandsPerEventLoop")
      (set-java ep conf "minTimeout")
      (set-java ep conf "queueInitialCapacity")
      (set-java ep conf "ticksPerWheel")
      ep)))

(defn ^ClientPolicy create-client-policy [event-loops conf]
  (when (get "batchPolicyDefault" conf)
    (throw (IllegalArgumentException. "batchPolicyDefault is not supported")))
  (when (get "infoPolicyDefault" conf)
    (throw (IllegalArgumentException. "infoPolicyDefault is not supported")))
  (when (get "queryPolicyDefault" conf)
    (throw (IllegalArgumentException. "queryPolicyDefault is not supported")))
  (when (get "scanPolicyDefault" conf)
    (throw (IllegalArgumentException. "scanPolicyDefault is not supported")))
  (when (get "tlsPolicyDefault" conf)
    (throw (IllegalArgumentException. "tlsPolicyDefault is not supported")))

  (let [cp (ClientPolicy.)
        username (get conf "username")
        password (get conf "password")]
    (when (and username password)
      (set! (.user cp) username)
      (set! (.password cp) password))
    (when (nil? event-loops)
      (throw (ex-info "cannot use nil for event-loops" {:conf conf})))
    (set! (.eventLoops cp) event-loops)
    (set! (.readPolicyDefault cp) (get conf "readPolicyDefault" (map->policy conf)))
    (set! (.writePolicyDefault cp) (get conf "writePolicyDefault" (map->write-policy conf)))
    (set-java-enum cp conf "AuthMode")
    (set-java cp conf "clusterName")
    (set-java cp conf "connPoolsPerNode")
    (set-java cp conf "failIfNotConnected")
    (set-java cp conf "ipMap")
    (set-java cp conf "loginTimeout")
    (set-java cp conf "maxConnsPerNode")
    (set-java cp conf "maxSocketIdle")
    (set-java cp conf "rackAware")
    (set-java cp conf "rackId")
    (set-java cp conf "requestProleReplicas")
    (set-java cp conf "sharedThreadPool")
    (set-java cp conf "tendInterval")
    (set-java cp conf "threadPool")
    (set-java cp conf "timeout")
    (set-java cp conf "tlsPolicy")
    (set-java cp conf "useServicesAlternate")
    cp))
