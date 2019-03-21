(ns ^{:author "Ido Barkan"
      :doc "this test require a local aerospike. This can be achieved via docker:
           $ sudo docker pull aerospike
           $ sudo docker run -d --name aerospike -p 3000:3000 -p 3001:3001 -p 3002:3002 -p 3003:3003 aerospike"}
  aerospike-clj.client-test
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [aerospike-clj.client :as client]
            [aerospike-clj.policy :as policy]
            [cheshire.core :as json])
  (:import [com.aerospike.client AerospikeException Value]
           [com.aerospike.client.cdt ListOperation ListPolicy ListOrder ListWriteFlags ListReturnType
            MapOperation MapPolicy MapOrder MapWriteFlags MapReturnType]
           [com.aerospike.client.policy Priority ConsistencyLevel Replica AuthMode ClientPolicy GenerationPolicy
                                        RecordExistsAction]
           [java.util HashMap]
           [com.fasterxml.jackson.databind ObjectMapper]))

(def K "k")
(def K2 "k2")
(def K_not_exists "k_not_exists")
(def _set "set")
(def ^:dynamic *c* nil)

(defn- test-fixture [f]
  @(client/delete *c* K _set)
  @(client/delete *c* K2 _set)
  (f))

(defn db-connection [f]
  (binding [*c* (client/init-simple-aerospike-client
                  ["localhost"]
                  "test")]
    (f)
    (client/stop-aerospike-client *c*)))

(use-fixtures :each test-fixture)
(use-fixtures :once db-connection)

(deftest client-creation
  (let [c (client/init-simple-aerospike-client ["localhost"] "test")]
    (is c)
    (is (= "localhost" (:cluster-name c))))
  (is (thrown-with-msg? Exception #"setting maxCommandsInProcess>0 and maxCommandsInQueue=0 creates an unbounded delay queue"
                        (client/init-simple-aerospike-client ["localhost"] "test" {"maxCommandsInProcess" 1}))))

(deftest health
  (is (true? (client/healthy? *c* 10))))

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

(deftest put-get-clj-map
  (let [data {:foo {:bar [(rand-int 1000)]}}]
    (is (true? @(client/create *c* K _set data 100)))
    (testing "clojure maps can be serialized as-is")
    (let [v @(client/get-single-no-meta *c* K _set)]
      (is (= data v)) ;; per value it is identical
      (is (= clojure.lang.PersistentArrayMap (type v)))
      (testing "using Jackson to recursively create a PersistantHashMap"
        (let [json (.writeValueAsString (ObjectMapper.) v)
              parsed (json/parse-string json #(keyword (subs % 1)))]
          (is (= data parsed))
          (is (true? (map? parsed))))))))

(deftest put-multiple-bins-get-clj-map
  (let [data {:foo {:bar [(rand-int 1000)]}
              :baz true
              :qux false
              :quux nil}]
    (is (true? @(client/create *c* K _set data 100)))
    (testing "clojure maps can be serialized from bins")
    (let [v @(client/get-single-no-meta *c* K _set)]
      (is (= (:foo data) (:foo v))) ;; per value it is identical
      (is (= (:baz data) (:baz v))) ;; true value returns the same after being sanitized/desanitized
      (is (= (:qux data) (:qux v))) ;; false value returns the same after being sanitized/desanitized
      (is (= (:quux data) (:quux v))) ;; nil value retuns the same after being sanitized/desanitized
      (is (= clojure.lang.PersistentArrayMap (type v))) ;; converted back to a Clojure map instead of HashMap
      (is (true? (map? v)))
      (testing "using Jackson to recursively create a PersistantHashMap"
        (let [json (.writeValueAsString (ObjectMapper.) v)
              parsed (json/parse-string json #(keyword (subs % 1)))]
          (is (= data parsed))
          (is (true? (map? parsed))))))))

(deftest get-single-multiple-bins
  (let [data {:foo [(rand-int 1000)]
              :bar [(rand-int 1000)]
              :baz [(rand-int 1000)]}]
    (is (true? @(client/create *c* K _set data 100)))
    (testing "bin values can be retrieved individually and all together"
      (let [v1 @(client/get-single *c* K _set [:foo])
            v2 @(client/get-single *c* K _set [:bar])
            v3 @(client/get-single *c* K _set [:baz])
            v4 @(client/get-single *c* K _set [:all])]
        (is (= (:foo data) (:foo (:payload v1))))
        (is (= (:bar data) (:bar (:payload v2))))
        (is (= (:baz data) (:baz (:payload v3))))
        (is (= data (:payload v4)))
        (is (true? (map? (:payload v1))))))))

(deftest adding-bins-to-record
  (let [data {:foo [(rand-int 1000)]
              :bar [(rand-int 1000)]
              :baz [(rand-int 1000)]}
        new-data {:qux [(rand-int 1000)]}]
    (is (true? @(client/create *c* K _set data 100)))
    (is (true? @(client/add-bins *c* K _set new-data 100))) ;; adding value to bin
    (testing "bin values can be added to existing records"
      (let [v @(client/get-single-no-meta *c* K _set)]
        (is (= v (merge data new-data)))
        (is (contains? v :qux))))))

(deftest removing-bins-from-record
  (let [data {:foo [(rand-int 1000)]
              :bar [(rand-int 1000)]
              :baz [(rand-int 1000)]
              :qux [(rand-int 1000)]}
        bin-keys [:foo :bar :baz]]
    (is (true? @(client/create *c* K _set data 100)))
    (is (true? @(client/delete-bins *c* K _set bin-keys 100))) ;; removing value from bin
    (testing "bin values can be removed from existing records"
      (let [v @(client/get-single-no-meta *c* K _set)]
        (is (= v (apply dissoc data bin-keys)))
        (is (contains? v :qux))))))

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

(deftest too-long-bin-name
  (let [long-bin-name "thisstringislongerthan14characters"]
    (is (thrown-with-msg?
          Exception #"Bin names have to be <= 14 characters..."
          @(client/put *c* K _set {long-bin-name "foo"} 100)))))

(deftest update-with-wrong-gen
  (let [data (rand-int 1000)]
    (is (true? @(client/create *c* K _set data 100)))
    (let [{:keys [payload gen]} @(client/get-single *c* K _set)]
      (is (= data payload))
      (is (= 1 gen))
      (is (thrown-with-msg? AerospikeException #"Generation error"
                            @(client/update *c* K _set (inc data) 42 100))))))

(deftest put-create-only-raises-exception
  (is (true? @(client/create *c* K _set 1 100)))
  (is (thrown-with-msg? AerospikeException #"Key already exists"
                        @(client/create *c* K _set 1 100))))

(deftest put-replace-only-raises-exception
  (is (thrown-with-msg? AerospikeException #"Key not found"
                        @(client/replace-only *c* K_not_exists _set 1 100))))
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

(deftest put-multiple
  (let [data [(rand-int 1000) (rand-int 1000)]
        ks [K K2 "not-there"]]
    (is (= [true true]
           @(client/put-multiple *c* [K K2] (repeat _set) data (repeat 100))))
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
          @(client/get-multiple *c* ks (repeat _set)
                                {:transcoder (fn [res]
                                               (update res :payload #(json/parse-string % keyword)))})]
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
    (is (thrown-with-msg? Exception #"oh-no" @(client/get-single *c* K _set [:all] {:transcoder transcoder})))))

(deftest operations-lists
  (let [result1 @(client/operate *c* K _set 100
                                 [(ListOperation/append
                                    (ListPolicy. ListOrder/UNORDERED ListWriteFlags/ADD_UNIQUE)
                                    ""
                                    (Value/get "foo"))])
        result2 @(client/operate *c* K _set 100
                                 [(ListOperation/append
                                    (ListPolicy. ListOrder/UNORDERED ListWriteFlags/ADD_UNIQUE)
                                    ""
                                    (Value/get "bar"))])]
    (is (thrown-with-msg? AerospikeException
                          #"Map key exists"
                          @(client/operate *c* K _set 100
                                           [(ListOperation/append
                                              (ListPolicy. ListOrder/UNORDERED ListWriteFlags/ADD_UNIQUE)
                                              ""
                                              (Value/get "bar"))])))
    (let [result3 @(client/operate *c* K _set 100
                                   [(ListOperation/appendItems
                                      (ListPolicy. ListOrder/UNORDERED (bit-or ListWriteFlags/ADD_UNIQUE
                                                                               ListWriteFlags/PARTIAL
                                                                               ListWriteFlags/NO_FAIL))
                                      ""
                                      [(Value/get "bar") (Value/get "baz")])])
          result4 @(client/get-single *c* K _set)]
      (is (= 1 (:payload result1)))
      (is (= 2 (:payload result2)))
      (is (= 3 (:payload result3)))
      (is (= ["foo" "bar" "baz"] (vec (:payload result4)))))))

(deftest operations-maps
  (let [result1 @(client/operate *c* K _set 100
                                 [(MapOperation/put
                                    (MapPolicy. MapOrder/UNORDERED MapWriteFlags/DEFAULT)
                                    ""
                                    (Value/get "foo")
                                    (Value/get "foo1"))])
        result2 @(client/operate *c* K _set 100
                                 [(MapOperation/put
                                    (MapPolicy. MapOrder/UNORDERED MapWriteFlags/DEFAULT)
                                    ""
                                    (Value/get "bar")
                                    (Value/get "bar1"))])]
    (is (thrown-with-msg? AerospikeException
                          #"Map key exists"
                          @(client/operate *c* K _set 100
                                           [(MapOperation/put
                                              (MapPolicy. MapOrder/UNORDERED MapWriteFlags/CREATE_ONLY)
                                              ""
                                              (Value/get "foo")
                                              (Value/get "foo2"))])))
    (let [result3 @(client/operate *c* K _set 100
                                   [(MapOperation/putItems
                                      (MapPolicy. MapOrder/UNORDERED (bit-or MapWriteFlags/CREATE_ONLY
                                                                             MapWriteFlags/PARTIAL
                                                                             MapWriteFlags/NO_FAIL))
                                      ""
                                      {(Value/get "foo") (Value/get "foo2")
                                       (Value/get "baz") (Value/get "baz1")})])
          result4 @(client/get-single *c* K _set)]
      (is (= 1 (:payload result1)))
      (is (= 2 (:payload result2)))
      (is (= 3 (:payload result3)))
      (is (= {"foo" "foo1" "bar" "bar1" "baz" "baz1"} (into {} (:payload result4)))))))

(deftest operations-sets-maps-based
  (letfn [(set-add [v]
            (client/operate *c* K _set 100
                            [(MapOperation/put
                               (MapPolicy. MapOrder/UNORDERED (bit-or
                                                                MapWriteFlags/CREATE_ONLY
                                                                MapWriteFlags/NO_FAIL))
                               ""
                               (Value/get v)
                               (Value/get (byte 1)))]))
          (set-pop [v]
            (client/operate *c* K _set 100
                            [(MapOperation/removeByKey
                               ""
                               (Value/get v)
                               MapReturnType/KEY)]))
          (set-getall []
            (letfn [(->set [res] (->> ^HashMap (:payload res)
                                      .keySet
                                      (into #{})))]
              @(client/get-single *c* K _set [:all] {:transcoder ->set})))
          (set-size []
            (client/operate *c* K _set 100
                            [(MapOperation/size "")]))]

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
            (client/operate *c* K _set 100
                            [(ListOperation/append
                               (ListPolicy. ListOrder/UNORDERED (bit-or ListWriteFlags/ADD_UNIQUE
                                                                        ListWriteFlags/PARTIAL
                                                                        ListWriteFlags/NO_FAIL))
                               ""
                               (Value/get v))]))
          (set-pop [v]
            (client/operate *c* K _set 100
                            [(ListOperation/removeByValue
                               ""
                               (Value/get v)
                               ListReturnType/VALUE)]))
          (set-getall []
            (letfn [(->set [res] (->> res :payload (into #{})))]
              @(client/get-single *c* K _set [:all] {:transcoder ->set})))
          (set-size []
            (client/operate *c* K _set 100
                            [(ListOperation/size "")]))]

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
  (let [rp (.getReadPolicyDefault (client/get-client *c*))]
    (is (= Priority/DEFAULT (.priority rp))) ;; Priority of request relative to other transactions. Currently, only used for scans.
    (is (= ConsistencyLevel/CONSISTENCY_ONE (.consistencyLevel rp))) ;; Involve master only in the read operation.
    (is (= Replica/SEQUENCE (.replica rp))) ;; Try node containing master partition first.
    ;; If connection fails, all commands try nodes containing replicated partitions.
    ;; If socketTimeout is reached, reads also try nodes containing replicated partitions,
    ;; but writes remain on master node.)))
    ;; This option requires ClientPolicy.requestProleReplicas to be enabled in order to function properly.
    (is (= 30000 (.socketTimeout rp))) ;; 30 seconds default
    (is (zero? (.totalTimeout rp))) ;; no time limit
    (is (= 3000 (.timeoutDelay rp))) ;; no delay, connection closed on timeout
    (is (= 2 (.maxRetries rp))) ;; initial attempt + 2 retries = 3 attempts
    (is (zero? (.sleepBetweenRetries rp))) ;; do not sleep between retries
    (is (false? (.sendKey rp))) ;; do not send the user defined key
    (is (false? (.linearizeRead rp))))) ;; Force reads to be linearized for server namespaces that support strong consistency mode.

(deftest configure-read-policy
  (let [c (client/init-simple-aerospike-client
            ["localhost"] "test"
            {"readPolicyDefault" (policy/map->policy {"ConsistencyLevel" "CONSISTENCY_ALL"
                                                      "linearizeRead" true
                                                      "maxRetries" 1
                                                      "Replica" "RANDOM"
                                                      "sendKey" true
                                                      "sleepBetweenRetries" 100
                                                      "socketTimeout" 1000
                                                      "timeoutDelay" 2000
                                                      "totalTimeout" 3000})})
        rp (.getReadPolicyDefault (client/get-client c))]
    (is (= Priority/DEFAULT (.priority rp)))
    (is (= ConsistencyLevel/CONSISTENCY_ALL (.consistencyLevel rp)))
    (is (= Replica/RANDOM (.replica rp)))
    (is (= 1000 (.socketTimeout rp)))
    (is (= 3000 (.totalTimeout rp)))
    (is (= 2000 (.timeoutDelay rp)))
    (is (= 1 (.maxRetries rp)))
    (is (= 100 (.sleepBetweenRetries rp)))
    (is (true? (.sendKey rp)))
    (is (true? (.linearizeRead rp)))))

(deftest default-write-policy
  (let [rp (.getWritePolicyDefault (client/get-client *c*))]
    (is (= Priority/DEFAULT (.priority rp))) ;; Priority of request relative to other transactions. Currently, only used for scans.
    (is (= ConsistencyLevel/CONSISTENCY_ONE (.consistencyLevel rp))) ;; Involve master only in the read operation.
    (is (= Replica/SEQUENCE (.replica rp))) ;; Try node containing master partition first.
    ;; If connection fails, all commands try nodes containing replicated partitions.
    ;; If socketTimeout is reached, reads also try nodes containing replicated partitions,
    ;; but writes remain on master node.)))
    ;; This option requires ClientPolicy.requestProleReplicas to be enabled in order to function properly.
    (is (= 30000 (.socketTimeout rp))) ;; 30 seconds default
    (is (zero? (.totalTimeout rp))) ;; no time limit
    (is (= 3000 (.timeoutDelay rp))) ;; no delay, connection closed on timeout
    (is (= 2 (.maxRetries rp))) ;; initial attempt + 2 retries = 3 attempts
    (is (zero? (.sleepBetweenRetries rp))) ;; do not sleep between retries
    (is (false? (.sendKey rp))) ;; do not send the user defined key
    (is (false? (.linearizeRead rp))))) ;; Force reads to be linearized for server namespaces that support strong consistency mode.

(deftest configure-write-policy
  (let [c (client/init-simple-aerospike-client
            ["localhost"] "test"
            {"writePolicyDefault" (policy/map->write-policy {"CommitLevel" "COMMIT_MASTER"
                                                             "durableDelete" true
                                                             "expiration" 1000
                                                             "generation" 7
                                                             "GenerationPolicy" "EXPECT_GEN_GT"
                                                             "RecordExistsAction" "REPLACE_ONLY"
                                                             "respondAllOps" true})})

        wp (.getWritePolicyDefault (client/get-client c))]
    (is (= Priority/DEFAULT (.priority wp)))
    (is (= ConsistencyLevel/CONSISTENCY_ONE (.consistencyLevel wp)))
    (is (true? (.durableDelete wp)))
    (is (= 1000 (.expiration wp)))
    (is (= 7 (.generation wp)))
    (is (= GenerationPolicy/EXPECT_GEN_GT (.generationPolicy wp)))
    (is (= RecordExistsAction/REPLACE_ONLY (.recordExistsAction wp)))
    (is (true? (.respondAllOps wp)))))

