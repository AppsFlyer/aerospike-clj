(ns aerospike-clj.policy
  (:import [com.aerospike.client AerospikeClient]
           [com.aerospike.client.async EventPolicy]
    #_{:clj-kondo/ignore [:unused-import]}
           [com.aerospike.client.policy Policy ClientPolicy WritePolicy RecordExistsAction
                                        GenerationPolicy BatchPolicy CommitLevel
                                        AuthMode ReadModeAP ReadModeSC Replica BatchWritePolicy]))

(defmacro set-java [obj conf obj-name]
  `(when (some? (get ~conf ~obj-name))
     (set! (. ~obj ~(symbol obj-name)) (get ~conf ~obj-name))))

(defn lowercase-first [s]
  (apply str (Character/toLowerCase ^Character (first s)) (rest s)))

(defmacro set-java-enum [obj conf obj-name]
  `(when (get ~conf ~obj-name)
     (set! (. ~obj ~(symbol (lowercase-first obj-name)))
           (Enum/valueOf ~(symbol obj-name) (get ~conf ~obj-name)))))

(defn- throw-invalid-state [msg conf]
  (throw (ex-info msg {:conf (dissoc conf "password")})))

(defn map->policy
  "Create a (read) `Policy` from a map. Enumeration names should start with capitalized letter.
  This function is slow due to possible reflection."
  ^Policy [conf]
  (let [p    (Policy.)
        conf (merge {"timeoutDelay" 3000} conf)]
    (set-java-enum p conf "ReadModeAP")
    (set-java-enum p conf "ReadModeSC")
    (set-java p conf "maxRetries")
    (set-java-enum p conf "Replica")
    (set-java p conf "sendKey")
    (set-java p conf "sleepBetweenRetries")
    (set-java p conf "socketTimeout")
    (set-java p conf "timeoutDelay")
    (set-java p conf "totalTimeout")
    p))

(defn map->batch-write-policy
  "Create a `BatchWritePolicy` from a map. Enumeration names should start with capitalized letter.
  This function is slow due to possible reflection."
  ^BatchWritePolicy [conf]
  (let [p (BatchWritePolicy.)]
    (set-java-enum p conf "RecordExistsAction")
    (set-java-enum p conf "CommitLevel")
    (set-java-enum p conf "GenerationPolicy")
    (set-java p conf "filterExp")
    (set-java p conf "generation")
    (set-java p conf "expiration")
    (set-java p conf "durableDelete")
    (set-java p conf "sendKey")
    p))

(defn map->batch-policy
  "Create a `BatchPolicy` from a map.
  This function is slow due to possible reflection."
  ^BatchPolicy [conf]
  (let [bp   (BatchPolicy. (map->policy conf))
        conf (merge {"timeoutDelay" 3000} conf)]
    (set-java bp conf "allowInline")
    (set-java bp conf "respondAllKeys")
    (set-java bp conf "maxConcurrentThreads")
    (set-java bp conf "sendSetName")
    bp))

(defn map->write-policy
  "Create a `WritePolicy` from a map. Keys are strings identical to field names
  enum fields should start capitalized. This function is slow and involves reflection.
  For a faster creation use `write-policy` which uses the client policy caching."
  ^WritePolicy [conf]
  (let [wp (WritePolicy. (map->policy conf))]
    (set-java-enum wp conf "CommitLevel")
    (set-java wp conf "durableDelete")
    (set-java wp conf "expiration")
    (set-java wp conf "generation")
    (set-java-enum wp conf "GenerationPolicy")
    (set-java-enum wp conf "RecordExistsAction")
    (set-java wp conf "respondAllOps")
    wp))

(defn write-policy
  "Create a write policy to be passed to put methods via `{:policy wp}`.
  Also used in `update` and `create`.
  The default policy in case the record exists is `RecordExistsAction/REPLACE`."
  (^WritePolicy [client expiration]
   (write-policy client expiration (RecordExistsAction/REPLACE)))
  (^WritePolicy [client expiration record-exists-action]
   (let [wp (WritePolicy. (.getWritePolicyDefault ^AerospikeClient client))]
     (set! (.expiration wp) expiration)
     (set! (.recordExistsAction wp) record-exists-action)
     wp)))

(defn batch-write-policy
  "Create a write policy to be passed to put methods via `{:policy wp}`.
  Also used in `update` and `create`.
  The default policy in case the record exists is `RecordExistsAction/UPDATE`."
  (^BatchWritePolicy [client expiration]
   (batch-write-policy client expiration (RecordExistsAction/UPDATE)))
  (^BatchWritePolicy [client expiration record-exists-action]
   (let [wp (BatchWritePolicy. (.getBatchWritePolicyDefault ^AerospikeClient client))]
     (set! (.expiration wp) expiration)
     (set! (.recordExistsAction wp) record-exists-action)
     wp)))

(defn set-policy
  "Create a write policy with UPDATE record exists action.
   in case of new entry, create it
   in case the entry exists, update entry"
  ^WritePolicy [client expiration]
  (let [wp (write-policy client expiration)]
    (set! (.recordExistsAction wp) RecordExistsAction/UPDATE)
    wp))

(defn update-policy
  "Create a write policy with `expiration`, expected `generation`
  and EXPECT_GEN_EQUAL generation policy."
  ^WritePolicy [client generation new-expiration]
  (let [wp (write-policy client new-expiration)]
    (set! (.generation wp) generation)
    (set! (.generationPolicy wp) GenerationPolicy/EXPECT_GEN_EQUAL)
    wp))

(defn create-only-policy
  "Create a write policy with CREATE_ONLY record exists action."
  ^WritePolicy [client expiration]
  (let [wp (write-policy client expiration)]
    (set! (.recordExistsAction wp) RecordExistsAction/CREATE_ONLY)
    wp))

(defn replace-only-policy
  "Create a write policy with REPLACE_ONLY record exists action. Fails if the record does not exists"
  ^WritePolicy [client expiration]
  (let [wp (write-policy client expiration)]
    (set! (.recordExistsAction wp) RecordExistsAction/REPLACE_ONLY)
    wp))

(defn update-only-policy
  "Create a write policy with UPDATE_ONLY record exists action. The policy helps add/delete bins in
  records without replacing existing data."
  ^WritePolicy [client new-expiration]
  (let [wp (write-policy client new-expiration)]
    (set! (.recordExistsAction wp) RecordExistsAction/UPDATE_ONLY)
    wp))

(defn map->event-policy
  "Create an `EventPolicy` from a map. Usage same as `map->write-policy`."
  (^EventPolicy []
   (map->event-policy {}))
  (^EventPolicy [conf]
   (let [event-policy            (EventPolicy.)
         max-commands-in-process (get conf "maxCommandsInProcess" 0)
         max-commands-in-queue   (get conf "maxCommandsInQueue" 0)]
     (when (and (pos? max-commands-in-process)
                (zero? max-commands-in-queue))
       (throw-invalid-state
         "setting maxCommandsInProcess>0 and maxCommandsInQueue=0 creates an unbounded delay queue"
         conf))
     (set! (.maxCommandsInProcess event-policy) max-commands-in-process)
     (set! (.maxCommandsInQueue event-policy) max-commands-in-queue)
     (set-java event-policy conf "commandsPerEventLoop")
     (set-java event-policy conf "minTimeout")
     (set-java event-policy conf "queueInitialCapacity")
     (set-java event-policy conf "ticksPerWheel")
     event-policy)))

(defn create-client-policy ^ClientPolicy [event-loops conf]
  (when (get "infoPolicyDefault" conf)
    (throw (IllegalArgumentException. "infoPolicyDefault is not supported")))
  (when (get "queryPolicyDefault" conf)
    (throw (IllegalArgumentException. "queryPolicyDefault is not supported")))
  (when (get "scanPolicyDefault" conf)
    (throw (IllegalArgumentException. "scanPolicyDefault is not supported")))
  (when (get "tlsPolicyDefault" conf)
    (throw (IllegalArgumentException. "tlsPolicyDefault is not supported")))

  (let [cp       (ClientPolicy.)
        username (get conf "username")
        password (get conf "password")]
    (when (and username password)
      (set! (.user cp) username)
      (set! (.password cp) password))
    (when (nil? event-loops)
      (throw-invalid-state "cannot use nil for event-loops" conf))
    (set! (.eventLoops cp) event-loops)
    (set! (.readPolicyDefault cp) (get conf "readPolicyDefault" (map->policy conf)))
    (set! (.writePolicyDefault cp) (get conf "writePolicyDefault" (map->write-policy conf)))
    (set! (.batchPolicyDefault cp) (get conf "batchPolicyDefault" (map->batch-policy conf)))
    (set! (.batchParentPolicyWriteDefault cp) (get conf "batchParentPolicyWriteDefault" (map->batch-policy conf)))
    (set! (.batchWritePolicyDefault cp) (get conf "batchWritePolicyDefault" (map->batch-write-policy conf)))

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
    (set-java cp conf "sharedThreadPool")
    (set-java cp conf "tendInterval")
    (set-java cp conf "threadPool")
    (set-java cp conf "timeout")
    (set-java cp conf "tlsPolicy")
    (set-java cp conf "useServicesAlternate")
    cp))
