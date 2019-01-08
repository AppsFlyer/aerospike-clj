(ns ^{:author "Ido Barkan"
      :doc "this test require a local aerospike. This can be achieved via docker:
           $ sudo docker pull aerospike
           $ sudo docker run -d --name aerospike -p 3000:3000 -p 3001:3001 -p 3002:3002 -p 3003:3003 aerospike"}
  aerospike-clj.client-test
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [aerospike-clj.client :as client]
            [taoensso.timbre :as timbre]
            [cheshire.core :as json])
  (:import [com.aerospike.client AerospikeException Value]
           [com.aerospike.client.cdt ListOperation ListPolicy ListOrder ListWriteFlags ListReturnType
            MapOperation MapPolicy MapOrder MapWriteFlags MapReturnType]
           [com.aerospike.client.policy Priority ConsistencyLevel Replica AuthMode ClientPolicy]
           [java.util HashMap]))

(def K "k")
(def K2 "k2")
(def _set "set")
(def ^:dynamic *c* nil)

(defn- test-fixture [f]
  @(client/delete *c* K _set)
  @(client/delete *c* K2 _set)
  (f))

(defn db-connection [f]
  (binding [*c* (client/init-simple-aerospike-client
                  ["localhost"]
                  "test"
                  {:enable-logging true})]
    (f)
    (client/stop-aerospike-client *c*)))

(use-fixtures :each test-fixture)
(use-fixtures :once db-connection)

(deftest client-creation
  (let [c (client/init-simple-aerospike-client ["localhost"] "test")]
    (is c)
    (cond
      (:ac c)  (is (= "localhost" (:cluster-name c)))
      (:acs c) (is (= "localhost-localhost" (:cluster-name c)))))
  (is (thrown-with-msg? Exception #"setting maxCommandsInProcess>0 and maxCommandsInQueue=0 creates an unbounded delay queue"
                        (client/init-simple-aerospike-client ["localhost"] "test" {:max-commands-in-process 1}))))

(defn cluster-names  [c]
  (case (count (client/get-all-clients c))
    1 "localhost"
    2 "localhost-localhost"))

(deftest health
  (is (true? (client/healty? *c* 10))))

(deftest get-record
  (let [data (rand-int 1000)]
    (is (true? @(client/create *c* K _set data 100)))
    (let [{:keys [payload gen] } @(client/get-single *c* K _set)]
      (is (= data payload))
      (is (= 1 gen)))))

(deftest get-miss
  (let [{:keys [payload gen] } @(client/get-single *c* K _set)]
    (is (nil? payload))
    (is (nil? gen))))

(deftest get-single
  (let [data (rand-int 1000)]
    (is (true? @(client/create *c* K _set data 100)))
    (is (= data @(client/get-single-no-meta *c* K _set)))))

(deftest update-test
  (is (true? @(client/create *c* K _set 16 100)))
  (is (true? @(client/update *c* K _set 17 1 100)))
  (let [{:keys [payload gen]} @(client/get-single *c* K _set)]
    (is (= 17 payload))
    (is (= 2 gen))))

(deftest too-long-key
  (let [too-long-key (clojure.string/join "" (repeat (inc client/MAX_KEY_LENGTH) "k"))]
    (is (thrown-with-msg? Exception #"key is too long"
                          @(client/put *c* too-long-key _set 1 100)))))

(deftest update-with-wrong-gen
  (let [data (rand-int 1000)]
    (is (true? @(client/create *c* K _set data 100)))
    (let [{:keys [payload gen]} @(client/get-single *c* K _set)]
      (is (= data payload))
      (is (= 1 gen))
      (is (thrown-with-msg? AerospikeException #"Generation error"
                            @(client/update *c* K _set (inc data) 42 100))))))

(deftest put-create-only-raises-exception
  (is (true? @(client/create *c* K _set  1 100)))
  (is (thrown-with-msg? AerospikeException #"Key already exists"
                        @(client/create *c* K _set 1 100))))

(deftest delete
  (is (true? @(client/put *c* K _set 1 100)))
  (is (true? @(client/delete *c* K _set)))
  (is (false? @(client/delete *c* K _set)))
  (is (nil? @(client/get-single *c* K _set))))

(deftest get-multiple
  (let [data [(rand-int 1000) (rand-int 1000)]
        ks [K K2 "not-there"]]
    (is (true? @(client/create *c* K _set (first data) 100)))
    (is (true? @(client/create *c* K2 _set (second data) 100)))
    (let [[{d1 :payload g1 :gen} {d2 :payload g2 :gen} {d3 :payload g3 :gen}] @(client/get-multiple *c* ks (repeat _set))]
      (is (= d1 (first data)))
      (is (= d2 (second data)))
      (is (= 1 g1 g2))
      (is (nil? d3))
      (is (nil? g3)))))

(deftest get-multiple-transcoded
  (let [put-json (fn [k d] (client/put *c* k _set d 100 {:transcoder json/generate-string}))
        data [{:a (rand-int 1000)} {:a (rand-int 1000)}]
        ks [K K2]]
    (is (true? @(put-json K (first data))))
    (is (true? @(put-json K2 (second data))))
    (let [[{d1 :payload g1 :gen}
           {d2 :payload g2 :gen}]
          @(client/get-multiple *c* ks (repeat _set) (fn [res]
                                                       (update res :payload #(json/parse-string % keyword))))]
      (is (= d1 (first data)))
      (is (= d2 (second data)))
      (is (= 1 g1 g2)))))

(deftest exists
  (is (true? @(client/create *c* K _set 1 100)))
  (is (true? @(client/exists? *c* K _set)))
  (is (true? @(client/delete *c* K _set)))
  (is (false? @(client/exists? *c* K _set))))

(deftest touch
  (is (true? @(client/create *c* K _set 1 100)))
  (let [{:keys [ttl]} @(client/get-single *c* K _set)]
    (is (true? @(client/touch *c* K _set 1000)))
    (let [{new-ttl :ttl} @(client/get-single *c* K _set)]
      (is (< ttl new-ttl)))))

(deftest transcoder-failure
  (is (true? @(client/create *c* K _set 1 100)))
  (let [transcoder (fn [_] (throw (Exception. "oh-no")))]
    (is (thrown-with-msg? Exception #"oh-no" @(client/get-single *c* K _set {:transcoder transcoder})))))

(deftest operations-lists
  (let [result1 @(client/operate *c* K _set 100 :update
                                 (ListOperation/append
                                   (ListPolicy. ListOrder/UNORDERED ListWriteFlags/ADD_UNIQUE)
                                   ""
                                   (Value/get "foo")))
        result2 @(client/operate *c* K _set 100 :update
                                 (ListOperation/append
                                   (ListPolicy. ListOrder/UNORDERED ListWriteFlags/ADD_UNIQUE)
                                   ""
                                   (Value/get "bar")))]
    (is (thrown-with-msg? AerospikeException
                          #"Map key exists"
                          @(client/operate *c* K _set 100 :update
                                           (ListOperation/append
                                             (ListPolicy. ListOrder/UNORDERED ListWriteFlags/ADD_UNIQUE)
                                             ""
                                             (Value/get "bar")))))
    (let [result3 @(client/operate *c* K _set 100 :update
                                   (ListOperation/appendItems
                                     (ListPolicy. ListOrder/UNORDERED (bit-or ListWriteFlags/ADD_UNIQUE
                                                                              ListWriteFlags/PARTIAL
                                                                              ListWriteFlags/NO_FAIL))
                                     ""
                                     [(Value/get "bar") (Value/get "baz")]))
          result4 @(client/get-single *c* K _set)]
      (is (= 1 (:payload result1)))
      (is (= 2 (:payload result2)))
      (is (= 3 (:payload result3)))
      (is (= ["foo" "bar" "baz"] (vec (:payload result4)))))))

(deftest operations-maps
  (let [result1 @(client/operate *c* K _set 100 :update
                                 (MapOperation/put
                                   (MapPolicy. MapOrder/UNORDERED MapWriteFlags/DEFAULT)
                                   ""
                                   (Value/get "foo")
                                   (Value/get "foo1")))
        result2 @(client/operate *c* K _set 100 :update
                                 (MapOperation/put
                                   (MapPolicy. MapOrder/UNORDERED MapWriteFlags/DEFAULT)
                                   ""
                                   (Value/get "bar")
                                   (Value/get "bar1")))]
    (is (thrown-with-msg? AerospikeException
                          #"Map key exists"
                          @(client/operate *c* K _set 100 :update
                                           (MapOperation/put
                                             (MapPolicy. MapOrder/UNORDERED MapWriteFlags/CREATE_ONLY)
                                             ""
                                             (Value/get "foo")
                                             (Value/get "foo2")))))
    (let [result3 @(client/operate *c* K _set 100 :update
                                   (MapOperation/putItems
                                     (MapPolicy. MapOrder/UNORDERED (bit-or MapWriteFlags/CREATE_ONLY
                                                                            MapWriteFlags/PARTIAL
                                                                            MapWriteFlags/NO_FAIL))
                                     ""
                                     {(Value/get "foo") (Value/get "foo2")
                                      (Value/get "baz") (Value/get "baz1")}))
          result4 @(client/get-single *c* K _set)]
      (is (= 1 (:payload result1)))
      (is (= 2 (:payload result2)))
      (is (= 3 (:payload result3)))
      (is (= {"foo" "foo1" "bar" "bar1" "baz" "baz1"} (into {} (:payload result4)))))))

(deftest operations-sets-maps-based
  (letfn [(set-add [v]
            (client/operate *c* K _set 100 :update
                            (MapOperation/put
                              (MapPolicy. MapOrder/UNORDERED (bit-or
                                                               MapWriteFlags/CREATE_ONLY
                                                               MapWriteFlags/NO_FAIL))
                              ""
                              (Value/get v)
                              (Value/get (byte 1)))))
          (set-pop [v]
            (client/operate *c* K _set 100 :update
                            (MapOperation/removeByKey
                              ""
                              (Value/get v)
                              MapReturnType/KEY)))
          (set-getall []
            (letfn [(->set [res] (->> ^HashMap (:payload res)
                                      .keySet
                                      (into #{})))]
              @(client/get-single *c* K _set {:transcoder ->set})))
          (set-size []
            (client/operate *c* K _set 100 :update
                            (MapOperation/size
                              "")))]

    (is (= 1 (:payload @(set-add "foo"))))
    (is (= 1 (:payload @(set-size))))
    (is (= 2 (:payload @(set-add "bar"))))
    (is (= 2 (:payload @(set-size))))
    (is (= 2 (:payload @(set-add "foo"))))
    (is (= #{"foo" "bar"} (set-getall)))
    (is (= "foo" (str (:payload @(set-pop "foo")))))
    (is (nil? (:payload @(set-pop "foo"))))
    (is (= #{"bar"} (set-getall)))))

(deftest operations-sets-list-based
  (letfn [(set-add [v]
            (client/operate *c* K _set 100 :update
                            (ListOperation/append
                              (ListPolicy. ListOrder/UNORDERED (bit-or ListWriteFlags/ADD_UNIQUE
                                                                       ListWriteFlags/PARTIAL
                                                                       ListWriteFlags/NO_FAIL))
                              ""
                              (Value/get v))))
          (set-pop [v]
            (client/operate *c* K _set 100 :update
                            (ListOperation/removeByValue
                              ""
                              (Value/get v)
                              ListReturnType/VALUE)))
          (set-getall []
            (letfn [(->set [res] (->> res :payload (into #{})))]
              @(client/get-single *c* K _set {:transcoder ->set})))
          (set-size []
            (client/operate *c* K _set 100 :update
                            (ListOperation/size
                              "")))]

    (is (= 1 (:payload @(set-add "foo"))))
    (is (= 1 (:payload @(set-size))))
    (is (= 2 (:payload @(set-add "bar"))))
    (is (= 2 (:payload @(set-size))))
    (is (= 2 (:payload @(set-add "foo"))))
    (is (= #{"foo" "bar"} (set-getall)))
    (is (= "foo" (str (first (:payload @(set-pop "foo"))))))
    (is (nil? (first (:payload @(set-pop "foo")))))
    (is (= #{"bar"} (set-getall)))))

(deftest default-read-policy
  (let [rp (client/get-default-read-policy *c*)]
    (is (= Priority/DEFAULT (.priority rp))) ;; Priority of request relative to other transactions. Currently, only used for scans.
    (is (= ConsistencyLevel/CONSISTENCY_ONE (.consistencyLevel rp))) ;; Involve master only in the read operation.
    (is (= Replica/SEQUENCE (.replica rp))) ;; Try node containing master partition first.
    ;; If connection fails, all commands try nodes containing replicated partitions.
    ;; If socketTimeout is reached, reads also try nodes containing replicated partitions,
    ;; but writes remain on master node.)))
    ;; This option requires ClientPolicy.requestProleReplicas to be enabled in order to function properly.
    (is (zero? (.socketTimeout rp))) ;; no socket idle time limit
    (is (zero? (.totalTimeout rp))) ;; no time limit
    (is (= 3000 (.timeoutDelay rp))) ;; no delay, connection closed on timeout
    (is (= 2 (.maxRetries rp))) ;; initial attempt + 2 retries = 3 attempts
    (is (zero? (.sleepBetweenRetries rp))) ;; do not sleep between retries
    (is (false? (.sendKey rp))) ;; do not send the user defined key
    (is (false? (.linearizeRead rp))))) ;; Force reads to be linearized for server namespaces that support strong consistency mode.

(deftest default-write-policy
  (let [rp (client/get-default-write-policy *c*)]
    (is (= Priority/DEFAULT (.priority rp))) ;; Priority of request relative to other transactions. Currently, only used for scans.
    (is (= ConsistencyLevel/CONSISTENCY_ONE (.consistencyLevel rp))) ;; Involve master only in the read operation.
    (is (= Replica/SEQUENCE (.replica rp))) ;; Try node containing master partition first.
    ;; If connection fails, all commands try nodes containing replicated partitions.
    ;; If socketTimeout is reached, reads also try nodes containing replicated partitions,
    ;; but writes remain on master node.)))
    ;; This option requires ClientPolicy.requestProleReplicas to be enabled in order to function properly.
    (is (zero? (.socketTimeout rp))) ;; no socket idle time limit
    (is (zero? (.totalTimeout rp))) ;; no time limit
    (is (= 3000 (.timeoutDelay rp))) ;; no delay, connection closed on timeout
    (is (zero? (.maxRetries rp))) ;; initial attempt + 2 retries = 3 attempts
    (is (zero? (.sleepBetweenRetries rp))) ;; do not sleep between retries
    (is (false? (.sendKey rp))) ;; do not send the user defined key
    (is (false? (.linearizeRead rp))))) ;; Force reads to be linearized for server namespaces that support strong consistency mode.

(deftest get-client-policy
  (let [^ClientPolicy cp (client/get-client-policy *c*)]
    (is (= AuthMode/INTERNAL (.authMode cp))) ;; Authentication mode used when user/password is defined.
    (is (some? (.batchPolicyDefault cp))) ;; Default batch policy that is used when batch command's policy is null.
    (is (nil? (.clusterName cp))) ;; Expected cluster name.
    (is (= 1 (.connPoolsPerNode cp))) ;; Number of synchronous connection pools used for each node.
    (is (= (:el *c*) (.eventLoops cp))) ;; Optional event loops to use in asynchronous commands.
    (is (true? (.failIfNotConnected cp))) ;; Throw exception if all seed connections fail on cluster instantiation.
    (is (false? (.forceSingleNode cp))) ;; For testing purposes only.
    ; (is (some? (.InfoPolicyDefault cp))) ;; Default info policy that is used when info command's policy is null.
    (is (nil? (.ipMap cp))) ;; A IP translation table is used in cases where different clients use different server IP addresses.
    (is (= 5000 (.loginTimeout cp))) ;; Login timeout in milliseconds.
    (is (= 300 (.maxConnsPerNode cp))) ;; Maximum number of connections allowed per server node.
    (is (= 55 (.maxSocketIdle cp))) ;; Maximum socket idle in seconds.
    (is (nil? (.password cp))) ;; Password authentication to cluster.
    (is (some? (.queryPolicyDefault cp))) ;; Default query policy that is used when query command's policy is null.
    (is (false? (.rackAware cp))) ;; Track server rack data.
    (is (zero? (.rackId cp))) ;; Rack where this client instance resides.
    (is (some? (.readPolicyDefault cp))) ;; Default read policy that is used when read command's policy is null.
    (is (true? (.requestProleReplicas cp))) ;; Should prole replicas be requested from each server node in the cluster tend thread.
    (is (some? (.scanPolicyDefault cp))) ;; Default scan policy that is used when scan command's policy is null.
    (is (false? (.sharedThreadPool cp))) ;; Is threadPool shared between other client instances or classes.
    (is (= 1000 (.tendInterval cp))) ;; Interval in milliseconds between cluster tends by maintenance thread.
    (is (nil? (.threadPool cp))) ;; Underlying thread pool used in synchronous batch, scan, and query commands.
    (is (= 1000 (.timeout cp))) ;; Initial host connection timeout in milliseconds.
    (is (nil? (.tlsPolicy cp))) ;; TLS secure connection policy for TLS enabled servers.
    (is (nil? (.user cp))) ;; User authentication to cluster.
    (is (false? (.useServicesAlternate cp))) ;; Should use "services-alternate" instead of "services" in info request during cluster tending.
    (is (some? (.writePolicyDefault cp))))) ;; Default write policy that is used when write command's policy is null.
