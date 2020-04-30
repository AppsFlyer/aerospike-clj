(ns ^{:author "Ido Barkan"
      :doc "this test require a local aerospike. This can be achieved via docker:
           $ sudo docker pull aerospike
           $ sudo docker run -d --name aerospike -p 3000:3000 -p 3001:3001 -p 3002:3002 -p 3003:3003 aerospike"}
  aerospike-clj.client-test
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [aerospike-clj.client :as client]
            [aerospike-clj.policy :as policy]
            [aerospike-clj.key :as as-key]
            [cheshire.core :as json]
            [manifold.deferred :as d])
  (:import [com.aerospike.client AerospikeException Value AerospikeClient 
            AerospikeException$AsyncQueueFull]
           [com.aerospike.client.cdt ListOperation ListPolicy ListOrder ListWriteFlags ListReturnType
                                     MapOperation MapPolicy MapOrder MapWriteFlags MapReturnType CTX]
           [com.aerospike.client.policy Priority ReadModeSC ReadModeAP Replica GenerationPolicy RecordExistsAction
                                        WritePolicy BatchPolicy Policy]
           [java.util HashMap ArrayList]
           [clojure.lang PersistentArrayMap]))

(def _set "set")
(def _set2 "set2")
(def as-namespace "test")
(def ^:dynamic *c* nil)

(defn db-connection [f]
  (binding [*c* (client/init-simple-aerospike-client
                  ["localhost"]
                  as-namespace)]
    (f)
    (client/stop-aerospike-client *c*)))

(use-fixtures :once db-connection)

(deftest client-creation
  (let [c (client/init-simple-aerospike-client ["localhost"] "test")]
    (is c)
    (is (= "localhost" (:cluster-name c))))
  (is (thrown-with-msg? Exception #"setting maxCommandsInProcess>0 and maxCommandsInQueue=0 creates an unbounded delay queue"
                        (client/init-simple-aerospike-client ["localhost"] "test" {"maxCommandsInProcess" 1}))))

(deftest info
  (doseq [node (client/get-nodes *c*)]
    (= {"health-stats" "stat=test_device_read_latency:value=0:device=/opt/aerospike/data/test.dat:namespace=test"
        "namespaces" "test"
        "set-config:context=service;enable-health-check=true" "ok"}
     @(client/info *c* node ["namespaces" "set-config:context=service;enable-health-check=true" "health-stats"]))))

(deftest health
  (is (true? (client/healthy? *c* 10))))

(defn random-key [] 
  (str (rand-int Integer/MAX_VALUE)))

(deftest get-record
  (let [data (rand-int 1000)
        k (random-key)]
    (is (true? @(client/create *c* k _set data 100)))
    (let [{:keys [payload gen] } @(client/get-single *c* k _set)]
      (is (= data payload))
      (is (= 1 gen)))))

(deftest get-record-by-digest
  (let [data (rand-int 1000)
        k (random-key)
        akey (as-key/create-key k as-namespace _set)
        digest (.digest akey)]
    (is (true? @(client/create *c* k _set data 100)))
    (let [{:keys [payload] } @(client/get-single *c* akey nil)]
      (is (= data payload)))
    (let [key-with-digest (as-key/create-key digest as-namespace _set (Value/get k))
          {:keys [payload] } @(client/get-single *c* key-with-digest nil)]
      (is (= data payload)))))

(deftest put-record-by-digest
  (let [data (rand-int 1000)
        k (random-key)
        akey (as-key/create-key k as-namespace _set)
        digest (.digest akey)
        key-with-digest (as-key/create-key digest as-namespace _set (Value/get k))]
    (is (true? @(client/create *c* key-with-digest nil data 100)))
    (let [{:keys [payload] } @(client/get-single *c* k _set)]
      (is (= data payload)))
    (let [key-with-digest (as-key/create-key digest as-namespace _set (Value/get k))
          {:keys [payload] } @(client/get-single *c* key-with-digest nil)]
      (is (= data payload)))))

(deftest get-miss
  (let [k (random-key)
        {:keys [payload gen] } @(client/get-single *c* k _set)]
    (is (nil? payload))
    (is (nil? gen))))

(deftest get-single
  (let [data (rand-int 1000)
        k (random-key)]
    (is (true? @(client/create *c* k _set data 100)))
    (is (= data @(client/get-single-no-meta *c* k _set)))))

(deftest get-batch
  (let [data (rand-int 1000)
        data2 (rand-int 1000)
        data3 (rand-int 1000)
        k (random-key) k2 (random-key) k3 (random-key)]
    (is (true? @(client/create *c* k _set data 100)))
    (is (true? @(client/create *c* k2 _set2 data2 100)))
    (is (true? @(client/create *c* k3 _set data3 100)))
    (let [brs [{:index k :set _set}
               {:index k2 :set _set2}
               {:index k2 :set _set}
               {:index k3 :set _set}
               {:index "not there" :set _set}]
          res @(client/get-batch *c* brs)]
      (is (= [data data2 nil data3 nil] (mapv :payload res)))
      (is (= [1 1 nil 1 nil] (mapv :gen res))))))

(deftest exists-batch
  (let [k (random-key)
        k2 (random-key)
        k3 (random-key)]
    (is (true? @(client/create *c* k _set 1 100)))
    (is (true? @(client/create *c* k2 _set2 1 100)))
    (is (true? @(client/create *c* k3 _set 1 100)))
    (let [indices [{:index k2 :set _set}
                   {:index k2 :set _set2}
                   {:index k3 :set _set}
                   {:index k :set _set}
                   {:index "not there" :set _set}]
          res @(client/exists-batch *c* indices)]
      (is (= [false true true true false] res)))))

(deftest put-get-clj-map
  (let [data {"foo" {"bar" [(rand-int 1000)]}}
        k (random-key)]
    (is (true? @(client/create *c* k _set data 100)))
    (testing "clojure maps can be serialized as-is"
      (let [v @(client/get-single-no-meta *c* k _set)]
        (is (= data v)) ;; per value it is identical
        (is (= PersistentArrayMap (type v)))))))

(deftest put-multiple-bins-get-clj-map
  (let [data {"foo" {"bar" [(rand-int 1000)]}
              "baz" true
              "qux" false
              "quuz" nil}
        k (random-key)]
    (is (true? @(client/create *c* k _set data 100)))
    (testing "clojure maps can be serialized from bins"
      (let [v @(client/get-single-no-meta *c* k _set)]
        (is (= (get data "foo") (get v "foo"))) ;; per value it is identical
        (is (= (get data "bar") (get v "bar"))) ;; true value returns the same after being sanitized/desanitized
        (is (= (get data "baz") (get v "baz"))) ;; false value returns the same after being sanitized/desanitized
        (is (= (get data "qux") (get v "qux"))) ;; nil value retuns the same after being sanitized/desanitized
        (is (= PersistentArrayMap (type v))) ;; converted back to a Clojure map instead of HashMap
        (is (true? (map? v)))))))

(deftest get-single-multiple-bins
  (let [data {"foo"  [(rand-int 1000)]
              "bar"  [(rand-int 1000)]
              "baz" [(rand-int 1000)]}
        k (random-key)]
    (is (true? @(client/create *c* k _set data 100)))
    (testing "bin values can be retrieved individually and all together"
      (let [v1 @(client/get-single *c* k _set {} ["foo"])
            v2 @(client/get-single *c* k _set {} ["bar"])
            v3 @(client/get-single *c* k _set {} ["baz"])
            v4 @(client/get-single *c* k _set {})] ;; getting all bins for the record
        (is (= (get data "foo") (get (:payload v1) "foo")))
        (is (= (get data "bar") (get (:payload v2) "bar")))
        (is (= (get data "baz") (get (:payload v3) "baz")))
        (is (= data (:payload v4)))
        (is (true? (map? (:payload v1))))))))

(deftest get-batch-multiple-bins
  (let [data {"foo"  [(rand-int 1000)]
              "bar"  [(rand-int 1000)]
              "baz" [(rand-int 1000)]}
        k (random-key)
        k2 (random-key)]
    (is (true? @(client/create *c* k _set data 100)))
    (is (true? @(client/create *c* k2 _set data 100)))
    (let [brs [{:index k :set _set :bins [:all]}
               {:index k :set _set}
               {:index k2 :set _set :bins ["bar"]}
               {:index "not there" :set _set}]
          res @(client/get-batch *c* brs)]
      (is (= [data data (select-keys data ["bar"]) nil] (mapv :payload res)))
      (is (= [1 1 1 nil] (mapv :gen res))))))

(deftest adding-bins-to-record
  (let [data {"foo" [(rand-int 1000)]
              "bar" [(rand-int 1000)]
              "baz" [(rand-int 1000)]}
        new-data {"qux" [(rand-int 1000)]}
        k (random-key)]
    (is (true? @(client/create *c* k _set data 100)))
    (is (true? @(client/add-bins *c* k _set new-data 100))) ;; adding value to bin
    (testing "bin values can be added to existing records"
      (let [v @(client/get-single-no-meta *c* k _set)]
        (is (= v (merge data new-data)))
        (is (= (into #{} (keys v)) #{"foo" "bar" "baz" "qux"}))))
    (testing "adding a bin that already exists in the record with a new value"
      (let [existing-bin {"foo" [(rand-int 1000)]}]
        (is (true? @(client/add-bins *c* k _set existing-bin 100)))
        (is (= (get existing-bin "foo")
               (get @(client/get-single-no-meta *c* k _set) "foo")))))))

(deftest removing-bins-from-record
  (let [data {"foo" [(rand-int 1000)]
              "bar" [(rand-int 1000)]
              "baz" [(rand-int 1000)]
              "qux" [(rand-int 1000)]}
        bin-keys ["foo" "bar" "baz"]
        k (random-key)]
    (is (true? @(client/create *c* k _set data 100)))
    (is (true? @(client/delete-bins *c* k _set bin-keys 100))) ;; removing value from bin
    (testing "bin values can be removed from existing records"
      (let [v @(client/get-single-no-meta *c* k _set)]
        (is (= v (apply dissoc data bin-keys)))
        (is (= (keys v) ["qux"]))))))

(deftest update-test
  (let [k (random-key)]
    (is (true? @(client/create *c* k _set 16 100)))
    (is (true? @(client/update *c* k _set 17 1 100)))
    (let [{:keys [payload gen]} @(client/get-single *c* k _set)]
      (is (= 17 payload))
      (is (= 2 gen)))))

(deftest too-long-bin-name
  (let [long-bin-name "thisstringislongerthan14characters"
        k (random-key)]
    (is (thrown-with-msg?
          Exception #"Bin names have to be <= 14 characters..."
          @(client/put *c* k _set {long-bin-name "foo"} 100)))))

(deftest update-with-wrong-gen
  (let [data (rand-int 1000)
        k (random-key)]
    (is (true? @(client/create *c* k _set data 100)))
    (let [{:keys [payload gen]} @(client/get-single *c* k _set)]
      (is (= data payload))
      (is (= 1 gen))
      (is (thrown-with-msg? AerospikeException #"Generation error"
                            @(client/update *c* k _set (inc data) 42 100))))))

(deftest put-create-only-raises-exception
  (let [k (random-key)]
    (is (true? @(client/create *c* k _set 1 100)))
    (is (thrown-with-msg? AerospikeException #"Key already exists"
                          @(client/create *c* k _set 1 100)))))

(deftest put-replace-only-raises-exception
  (is (thrown-with-msg? AerospikeException #"Key not found"
                        @(client/replace-only *c* "not here" _set 1 100))))
(deftest delete
  (let [k (random-key)]
    (is (true? @(client/put *c* k _set 1 100)))
    (is (true? @(client/delete *c* k _set)))
    (is (false? @(client/delete *c* k _set)))
    (is (nil? @(client/get-single *c* k _set)))))

(deftest get-multiple
  (let [data [(rand-int 1000) (rand-int 1000)]
        k (random-key)      
        k2 (random-key)
        ks [k k2 "not-there"]]
    (is (true? @(client/create *c* k _set (first data) 100)))
    (is (true? @(client/create *c* k2 _set (second data) 100)))
    (let [[{d1 :payload g1 :gen} {d2 :payload g2 :gen} {d3 :payload g3 :gen}] @(client/get-multiple *c* ks (repeat _set))]
      (is (= d1 (first data)))
      (is (= d2 (second data)))
      (is (= 1 g1 g2))
      (is (nil? d3))
      (is (nil? g3)))))

(deftest put-multiple
  (let [data [(rand-int 1000) (rand-int 1000)]
        k (random-key)
        k2 (random-key)]
    (is (= [true true]
           @(client/put-multiple *c* [k k2] (repeat _set) data (repeat 100))))
    (let [[{d1 :payload g1 :gen} {d2 :payload g2 :gen} {d3 :payload g3 :gen}] 
          @(client/get-batch *c* [{:index k :set _set}
                                  {:index k2 :set _set}
                                  {:index "not-there" :set _set}])]
      (is (= d1 (first data)))
      (is (= d2 (second data)))
      (is (= 1 g1 g2))
      (is (nil? d3))
      (is (nil? g3)))))

(deftest get-multiple-transcoded
  (let [put-json (fn [k d] (client/put *c* k _set d 100 {:transcoder json/generate-string}))
        data [{:a (rand-int 1000)} {:a (rand-int 1000)}]
        k (random-key)
        k2 (random-key)
        ks [k k2]]
    (is (true? @(put-json k (first data))))
    (is (true? @(put-json k2 (second data))))
    (let [[{d1 :payload g1 :gen}
           {d2 :payload g2 :gen}]
          @(client/get-multiple *c* ks (repeat _set)
                             {:transcoder (fn [res]
                                             (update res :payload #(json/parse-string % keyword)))})]
      (is (= d1 (first data)))
      (is (= d2 (second data)))
      (is (= 1 g1 g2)))))

(deftest exists
  (let [k (random-key)]
    (is (true? @(client/create *c* k _set 1 100)))
    (is (true? @(client/exists? *c* k _set)))
    (is (true? @(client/delete *c* k _set)))
    (is (false? @(client/exists? *c* k _set)))))

(deftest touch
  (let [k (random-key)]
    (is (true? @(client/create *c* k _set 1 100)))
    (let [{:keys [ttl]} @(client/get-single *c* k _set)]
      (is (true? @(client/touch *c* k _set 1000)))
      (let [{new-ttl :ttl} @(client/get-single *c* k _set)]
        (is (< ttl new-ttl))))))

(deftest transcoder-failure
  (let [k (random-key)]
    (is (true? @(client/create *c* k _set 1 100)))
    (let [transcoder (fn [_] (throw (Exception. "oh-no")))]
      (is (thrown-with-msg? Exception #"oh-no" @(client/get-single *c* k _set {:transcoder transcoder}))))))

(def empty-ctx-varargs (into-array CTX []))

(deftest operations-lists
  (let [k (random-key)
        result1 @(client/operate *c* k _set 100
                                 [(ListOperation/append
                                    (ListPolicy. ListOrder/UNORDERED ListWriteFlags/ADD_UNIQUE)
                                    ""
                                    (Value/get "foo")
                                    empty-ctx-varargs)])
        result2 @(client/operate *c* k _set 100
                                 [(ListOperation/append
                                    (ListPolicy. ListOrder/UNORDERED ListWriteFlags/ADD_UNIQUE)
                                    ""
                                    (Value/get "bar")
                                    empty-ctx-varargs)])]
    (is (thrown-with-msg? AerospikeException
                          #"Map key exists"
                          @(client/operate *c* k _set 100
                                           [(ListOperation/append
                                              (ListPolicy. ListOrder/UNORDERED ListWriteFlags/ADD_UNIQUE)
                                              ""
                                              (Value/get "bar")
                                              empty-ctx-varargs)])))
    (let [result3 @(client/operate *c* k _set 100
                                   [(ListOperation/appendItems
                                      (ListPolicy. ListOrder/UNORDERED (bit-or ListWriteFlags/ADD_UNIQUE
                                                                               ListWriteFlags/PARTIAL
                                                                               ListWriteFlags/NO_FAIL))
                                      ""
                                      [(Value/get "bar") (Value/get "baz")]
                                      empty-ctx-varargs)])
          result4 @(client/get-single *c* k _set)]
      (is (= 1 (:payload result1)))
      (is (= 2 (:payload result2)))
      (is (= 3 (:payload result3)))
      (is (= ["foo" "bar" "baz"] (vec (:payload result4)))))))

(deftest operations-windowed-uniq-list
  (let [outer-key (random-key)
        get-all #(:payload @(client/get-single *c* outer-key _set))
        key-ctx (fn [^String k]
                  (into-array CTX [(CTX/mapKey (Value/get k))]))
        outer-ctx (into-array CTX [])
        outer-map-policy (MapPolicy. MapOrder/KEY_ORDERED (bit-or MapWriteFlags/CREATE_ONLY
                                                                  MapWriteFlags/NO_FAIL))
        bin-name ""
        max-entries 4
        initial-empty-value #(hash-map (Value/get %) (Value/get (ArrayList. 1)))
        append (fn [k v]
                 (first 
                   (:payload 
                     @(client/operate 
                        *c* outer-key _set 100
                        [(MapOperation/putItems outer-map-policy bin-name (initial-empty-value k) outer-ctx)
                         (ListOperation/append bin-name (Value/get v) (key-ctx k))
                         (ListOperation/removeByRankRange bin-name -1 1 ListReturnType/INVERTED (key-ctx k))
                         (MapOperation/removeByRankRange
                           bin-name
                           (- max-entries)
                           max-entries
                           (bit-or MapReturnType/INVERTED
                                   MapReturnType/COUNT)
                           outer-ctx)]))))]

    (is (nil? (get-all)))
    (is (= 1 (append "foo" 19)))
    (is (= {"foo" [19]} (get-all)))
    (is (= 2 (append "bar" 18)))
    (is (= {"bar" [18] "foo" [19]} (get-all)))
    (is (= 2 (append "foo" 17)))
    (is (= {"bar" [18] "foo" [19]} (get-all)))
    (is (= 3 (append "baz" 15)))
    (is (= {"baz" [15] "bar" [18] "foo" [19]} (get-all)))
    (is (= 3 (append "baz" 16)))
    (is (= {"baz" [16] "bar" [18] "foo" [19]} (get-all)))
    (is (= 4 (append "bat" 10)))
    (is (= {"bat" [10] "baz" [16] "bar" [18] "foo" [19]} (get-all)))
    (is (= 5 (append "too-many" 11)))
    (is (= {"too-many" [11] "baz" [16] "bar" [18] "foo" [19]} (get-all)))
    (is (= 5 (append "way-too-many" 10)))
    (is (= {"too-many" [11] "baz" [16] "bar" [18] "foo" [19]} (get-all)))))

(deftest operations-maps
  (let [k (random-key)
        result1 @(client/operate *c* k _set 100
                                 [(MapOperation/put
                                    (MapPolicy. MapOrder/UNORDERED MapWriteFlags/DEFAULT)
                                    ""
                                    (Value/get "foo")
                                    (Value/get "foo1")
                                    empty-ctx-varargs)])
        result2 @(client/operate *c* k _set 100
                                 [(MapOperation/put
                                    (MapPolicy. MapOrder/UNORDERED MapWriteFlags/DEFAULT)
                                    ""
                                    (Value/get "bar")
                                    (Value/get "bar1")
                                    empty-ctx-varargs)])]
    (is (thrown-with-msg? AerospikeException
                          #"Map key exists"
                          @(client/operate *c* k _set 100
                                           [(MapOperation/put
                                              (MapPolicy. MapOrder/UNORDERED MapWriteFlags/CREATE_ONLY)
                                              ""
                                              (Value/get "foo")
                                              (Value/get "foo2")
                                              empty-ctx-varargs)])))
    (let [result3 @(client/operate *c* k _set 100
                                   [(MapOperation/putItems
                                      (MapPolicy. MapOrder/UNORDERED (bit-or MapWriteFlags/CREATE_ONLY
                                                                             MapWriteFlags/PARTIAL
                                                                             MapWriteFlags/NO_FAIL))
                                      ""
                                      {(Value/get "foo") (Value/get "foo2")
                                       (Value/get "baz") (Value/get "baz1")}
                                      empty-ctx-varargs)])
          result4 @(client/get-single *c* k _set)]
      (is (= 1 (:payload result1)))
      (is (= 2 (:payload result2)))
      (is (= 3 (:payload result3)))
      (is (= {"foo" "foo1" "bar" "bar1" "baz" "baz1"} (into {} (:payload result4)))))))

(deftest operations-sets-maps-based
  (let [k (random-key)]
    (letfn [(set-add [v]
              (client/operate *c* k _set 100
                              [(MapOperation/put
                                 (MapPolicy. MapOrder/UNORDERED (bit-or
                                                                  MapWriteFlags/CREATE_ONLY
                                                                  MapWriteFlags/NO_FAIL))
                                 ""
                                 (Value/get v)
                                 (Value/get (byte 1))
                                 empty-ctx-varargs)]))
            (set-pop [v]
              (client/operate *c* k _set 100
                              [(MapOperation/removeByKey
                                 ""
                                 (Value/get v)
                                 MapReturnType/KEY
                                 empty-ctx-varargs)]))
            (set-getall []
              (letfn [(->set [res] (->> ^HashMap (:payload res)
                                        .keySet
                                        (into #{})))]
                @(client/get-single *c* k _set {:transcoder ->set})))
            (set-size []
              (client/operate *c* k _set 100
                              [(MapOperation/size "" empty-ctx-varargs)]))]

      (is (= 1 (:payload @(set-add "foo"))))
      (is (= 1 (:payload @(set-size))))
      (is (= 2 (:payload @(set-add "bar"))))
      (is (= 2 (:payload @(set-size))))
      (is (= 2 (:payload @(set-add "foo"))))
      (is (= #{"foo" "bar"} (set-getall)))
      (is (= "foo" (str (:payload @(set-pop "foo")))))
      (is (nil? (:payload @(set-pop "foo"))))
      (is (= #{"bar"} (set-getall))))))

(deftest operations-sets-list-based
  (let [k (random-key)]
    (letfn [(set-add [v]
              (client/operate *c* k _set 100
                              [(ListOperation/append
                                 (ListPolicy. ListOrder/UNORDERED (bit-or ListWriteFlags/ADD_UNIQUE
                                                                          ListWriteFlags/PARTIAL
                                                                          ListWriteFlags/NO_FAIL))
                                 ""
                                 (Value/get v)
                                 empty-ctx-varargs)]))
            (set-pop [v]
              (client/operate *c* k _set 100
                              [(ListOperation/removeByValue
                                 ""
                                 (Value/get v)
                                 ListReturnType/VALUE
                                 empty-ctx-varargs)]))
            (set-getall []
              (letfn [(->set [res] (->> res :payload (into #{})))]
                @(client/get-single *c* k _set {:transcoder ->set})))
            (set-size []
              (client/operate *c* k _set 100
                              [(ListOperation/size "" empty-ctx-varargs)]))]

      (is (= 1 (:payload @(set-add "foo"))))
      (is (= 1 (:payload @(set-size))))
      (is (= 2 (:payload @(set-add "bar"))))
      (is (= 2 (:payload @(set-size))))
      (is (= 2 (:payload @(set-add "foo"))))
      (is (= #{"foo" "bar"} (set-getall)))
      (is (= "foo" (str (first (:payload @(set-pop "foo"))))))
      (is (nil? (first (:payload @(set-pop "foo")))))
      (is (= #{"bar"} (set-getall))))))

(deftest default-read-policy
  (let [rp (.getReadPolicyDefault ^AerospikeClient (client/get-client *c*))]
    (is (= Priority/DEFAULT (.priority rp))) ;; Priority of request relative to other transactions. Currently, only used for scans.
    (is (= ReadModeAP/ONE (.readModeAP rp))) ;; Involve single node in the read operation.
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
    (is (= ReadModeSC/SESSION (.readModeSC rp))))) ;; Ensures this client will only see an increasing sequence of record versions. Server only reads from master. This is the default..

(deftest configure-read-and-batch-policy
  (let [c (client/init-simple-aerospike-client
            ["localhost"] "test"
            {"readPolicyDefault" (policy/map->policy {"ReadModeAP" "ALL"
                                                      "ReadModeSC" "LINEARIZE"
                                                      "maxRetries" 1
                                                      "Replica" "RANDOM"
                                                      "sendKey" true
                                                      "sleepBetweenRetries" 100
                                                      "socketTimeout" 1000
                                                      "timeoutDelay" 2000
                                                      "totalTimeout" 3000})
             "batchPolicyDefault" (policy/map->batch-policy {"allowInline" false
                                                             "maxConcurrentThreads" 2
                                                             "sendSetName" true})})


        rp ^Policy (.getReadPolicyDefault (client/get-client c))
        bp ^BatchPolicy (.getBatchPolicyDefault (client/get-client c))]
    (is (= Priority/DEFAULT (.priority rp)))
    (is (= ReadModeAP/ALL (.readModeAP rp)))
    (is (= Replica/RANDOM (.replica rp)))
    (is (= 1000 (.socketTimeout rp)))
    (is (= 3000 (.totalTimeout rp)))
    (is (= 2000 (.timeoutDelay rp)))
    (is (= 1 (.maxRetries rp)))
    (is (= 100 (.sleepBetweenRetries rp)))
    (is (true? (.sendKey rp)))
    (is (= ReadModeSC/LINEARIZE (.readModeSC rp)))

    (is (false? (.allowInline bp)))
    (is (= 2 (.maxConcurrentThreads bp)))
    (is (true? (.sendSetName bp)))))

(deftest default-write-policy
  (let [rp ^WritePolicy (.getWritePolicyDefault (client/get-client *c*))]
    (is (= Priority/DEFAULT (.priority rp))) ;; Priority of request relative to other transactions. Currently, only used for scans.
    (is (= ReadModeAP/ONE (.readModeAP rp))) ;; Involve master only in the read operation.
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
    (is (= ReadModeSC/SESSION (.readModeSC rp))))) ;; Ensures this client will only see an increasing sequence of record versions. Server only reads from master. This is the default..

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

        wp ^WritePolicy (.getWritePolicyDefault (client/get-client c))]
    (is (= Priority/DEFAULT (.priority wp)))
    (is (= ReadModeAP/ONE (.readModeAP wp)))
    (is (true? (.durableDelete wp)))
    (is (= 1000 (.expiration wp)))
    (is (= 7 (.generation wp)))
    (is (= GenerationPolicy/EXPECT_GEN_GT (.generationPolicy wp)))
    (is (= RecordExistsAction/REPLACE_ONLY (.recordExistsAction wp)))
    (is (true? (.respondAllOps wp)))))


(deftest set-entry
  (let [data (rand-int 1000)
        update-data (rand-int 1000)
        k (random-key)]
    (is (true? @(client/set-single *c* k _set data 100)))
    (is (= data @(client/get-single-no-meta *c* k _set)))
    (is (true? @(client/set-single *c* k _set update-data 100)))
    (is (= update-data @(client/get-single-no-meta *c* k _set)))))

(deftest scan-test
  (let [conf {:policy (policy/map->write-policy {"sendKey" true})}
        aero-namespace "test"
        ttl 10
        k (random-key) k2 (random-key) k3 (random-key)
        ks #{k k2 k3}
        delete-records (fn []
                         @(client/delete *c* k _set)
                         @(client/delete *c* k2 _set)
                         @(client/delete *c* k3 _set))]

    (testing "it should throw an IllegalArgumentException when:
             conf is missing, :callback is missing, or :callback is not a function"
      (is (thrown? IllegalArgumentException 
                   @(client/scan-set *c* aero-namespace _set nil)))
      (is (thrown? IllegalArgumentException 
                   @(client/scan-set *c* aero-namespace _set {})))
      (is (thrown? IllegalArgumentException 
                   @(client/scan-set *c* aero-namespace _set {:callback "not a function"}))))

    (testing "it should throw a ClassCastException when :bins is not a vector"
      (is (thrown? ClassCastException 
                   @(client/scan-set *c* aero-namespace _set {:callback (constantly true) :bins {}}))))

    (testing "it should return all the items in the set"
      @(client/put-multiple *c* [k k2 k3] (repeat _set) [10 20 30] (repeat ttl) conf)

      (let [res (atom [])
            callback (fn [k v]
                       (when (ks (str k))
                         (swap! res conj [(.toString ^Value k) (:payload v)])))]

        @(client/scan-set *c* aero-namespace _set {:callback callback})
        (is (= (sort-by first @res)
               (sort-by first [[k 10] [k2 20] [k3 30]]))))

      (delete-records))

    (testing "it should return only the bins that were requested"
      (let [data [{"name" "John" "occupation" "Carpenter"}
                  {"name" "Jerry" "occupation" "Bus Driver"}
                  {"name" "Jack" "occupation" "Chef"}]]

        @(client/put-multiple *c* [k k2 k3] (repeat _set) data (repeat ttl) conf)

        (let [res (atom [])
              callback (fn [k v]
                         (when (ks (str k)) 
                           (swap! res conj [(.toString ^Value k) (:payload v)])))]

          @(client/scan-set *c* aero-namespace _set {:callback callback :bins ["occupation"]})

          (is (= (sort-by first @res)
                 (sort-by first
                   [[k {"occupation" "Carpenter"}]
                    [k2 {"occupation" "Bus Driver"}]
                    [k3 {"occupation" "Chef"}]]))))
        (delete-records)))

    (testing "it can update items during a scan"
      (let [client *c*
            callback (fn [k v]
                       (when (ks (str k))
                         (client/put client (.toString ^Value k) _set (inc (:payload v)) ttl)))]

        @(client/put-multiple *c* [k k2 k3] (repeat _set) [10 20 30] (repeat ttl) conf)

        @(client/scan-set *c* aero-namespace _set {:callback callback})

        (let [res @(client/get-batch *c* [{:index k :set _set}
                                          {:index k2 :set _set}
                                          {:index k3 :set _set}])]

          (is (= (sort (mapv :payload res)) [11 21 31]))))
      (delete-records))

    (testing "it can delete items during a scan"
      (let [client *c*
            callback (fn [k _]
                       (when (ks (str k)) 
                         (client/delete client (.toString ^Value k) _set)))]

        @(client/put-multiple *c* [k k2 k3] (repeat _set) [10 20 30] (repeat ttl) conf)

        @(client/scan-set *c* aero-namespace _set {:callback callback})

        (is (empty? (filter :payload @(client/get-batch *c* [{:index k :set _set}
                                                             {:index k2 :set _set}
                                                             {:index k3 :set _set}])))))
      (delete-records))

    (testing "it should stop the scan when the callback returns :abort-scan"
      @(client/put-multiple *c* [k k2 k3] (repeat _set) [10 20 30] (repeat ttl) conf)

      (let [res (atom [])
            counter (atom 0)
            callback (fn [k v]
                       (when (ks (str k))
                         (if (< (swap! counter inc) 2)
                           (swap! res conj [(.toString ^Value k) (:payload v)])
                           :abort-scan)))]

        (is (false? @(client/scan-set *c* aero-namespace _set {:callback callback})))
        (is (= 1 (count @res))))

      (delete-records))))



