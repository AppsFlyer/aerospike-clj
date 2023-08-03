(ns ^{:author      "Ido Barkan"
      :integration true}
  aerospike-clj.integration.integration-test
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [cheshire.core :as json]
            [aerospike-clj.integration.aerospike-setup :as as-setup]
            [aerospike-clj.client :as client]
            [aerospike-clj.protocols :as pt]
            [aerospike-clj.policy :as policy]
            [aerospike-clj.key :as as-key]
            [aerospike-clj.utils :as utils]
            [spy.core :as spy])
  (:import (com.aerospike.client Value AerospikeClient BatchWrite Operation Bin)
           (com.aerospike.client.cdt ListOperation ListPolicy ListOrder ListWriteFlags ListReturnType
                                     MapOperation MapPolicy MapOrder MapWriteFlags MapReturnType CTX)
           (com.aerospike.client.policy ReadModeSC ReadModeAP Replica GenerationPolicy RecordExistsAction
                                        WritePolicy BatchPolicy Policy CommitLevel BatchWritePolicy)
           (java.util HashMap ArrayList)
           (java.util.concurrent ExecutionException)
           (clojure.lang PersistentArrayMap)
           (aerospike_clj.client SimpleAerospikeClient)
           (com.aerospike.client.exp Exp)))

(def _set "set")
(def _set2 "set2")
(def as-namespace "test")
(def ^:dynamic *c* nil)
(def ^:dynamic *as-hosts* nil)
(def TTL 5)


(defn with-db-connection [test-fn]
  (let [as-hosts [(as-setup/get-host-and-port)]]
    (binding [*c*        (client/init-simple-aerospike-client
                           as-hosts
                           as-namespace)
              *as-hosts* as-hosts]
      (test-fn)
      (pt/stop *c*))))

(use-fixtures :once as-setup/with-aerospike with-db-connection)

(deftest client-creation
  (let [c (client/init-simple-aerospike-client *as-hosts* as-namespace)]
    (is c)
    (is (= *as-hosts* (.-hosts ^SimpleAerospikeClient c))))

  (letfn [(no-password? [ex]
            (let [conf (:conf (ex-data ex))]
              (and conf (not (contains? conf "password")))))]
    (let [ex (is (thrown-with-msg? Exception #"unbounded delay queue" (client/init-simple-aerospike-client *as-hosts* as-namespace {"maxCommandsInProcess" 1})))]
      (is (no-password? ex)))

    (with-redefs [client/create-event-loops (constantly nil)]
      (let [ex (is (thrown-with-msg? Exception #"event-loops" (client/init-simple-aerospike-client *as-hosts* as-namespace)))]
        (is (no-password? ex))))))

(deftest health
  (is (true? (pt/healthy? *c* 10))))

(defn random-key []
  (str (random-uuid)))

(deftest get-record
  (let [data (rand-int 1000)
        k    (random-key)]
    (is (true? @(pt/create *c* k _set data TTL)))
    (let [{:keys [payload gen]} @(pt/get-single *c* k _set)]
      (is (= data payload))
      (is (= 1 gen)))))

(deftest get-record-by-digest
  (let [data   (rand-int 1000)
        k      (random-key)
        akey   (as-key/create-key k as-namespace _set)
        digest (.digest akey)]
    (is (true? @(pt/create *c* k _set data TTL)))
    (let [{:keys [payload]} @(pt/get-single *c* akey nil)]
      (is (= data payload)))
    (let [key-with-digest (as-key/create-key digest as-namespace _set (Value/get k))
          {:keys [payload]} @(pt/get-single *c* key-with-digest nil)]
      (is (= data payload)))))

(deftest put-record-by-digest
  (let [data            (rand-int 1000)
        k               (random-key)
        akey            (as-key/create-key k as-namespace _set)
        digest          (.digest akey)
        key-with-digest (as-key/create-key digest as-namespace _set (Value/get k))]
    (is (true? @(pt/create *c* key-with-digest nil data TTL)))
    (let [{:keys [payload]} @(pt/get-single *c* k _set)]
      (is (= data payload)))
    (let [key-with-digest (as-key/create-key digest as-namespace _set (Value/get k))
          {:keys [payload]} @(pt/get-single *c* key-with-digest nil)]
      (is (= data payload)))))

(deftest get-miss
  (let [k (random-key)
        {:keys [payload gen]} @(pt/get-single *c* k _set)]
    (is (nil? payload))
    (is (nil? gen))))

(deftest get-single
  (let [data (rand-int 1000)
        k    (random-key)]
    (is (true? @(pt/create *c* k _set data TTL)))
    (is (= data @(pt/get-single-no-meta *c* k _set)))))

(deftest get-batch
  (let [data  (rand-int 1000)
        data2 (rand-int 1000)
        data3 (rand-int 1000)
        k     (random-key) k2 (random-key) k3 (random-key)]
    (is (true? @(pt/create *c* k _set data TTL)))
    (is (true? @(pt/create *c* k2 _set2 data2 TTL)))
    (is (true? @(pt/create *c* k3 _set data3 TTL)))
    (let [brs [{:index k :set _set}
               {:index k2 :set _set2}
               {:index k2 :set _set}
               {:index k3 :set _set}
               {:index "not there" :set _set}]
          res @(pt/get-batch *c* brs)]
      (is (= [data data2 nil data3 nil] (mapv :payload res)))
      (is (= [1 1 nil 1 nil] (mapv :gen res)))
      (is (= [k k2 k2 k3 "not there"] (mapv :index res)))
      (is (= [_set _set2 _set _set _set] (mapv :set res))))))

(deftest exists-batch
  (let [k  (random-key)
        k2 (random-key)
        k3 (random-key)]
    (is (true? @(pt/create *c* k _set 1 TTL)))
    (is (true? @(pt/create *c* k2 _set2 1 TTL)))
    (is (true? @(pt/create *c* k3 _set 1 TTL)))
    (let [indices [{:index k2 :set _set}
                   {:index k2 :set _set2}
                   {:index k3 :set _set}
                   {:index k :set _set}
                   {:index "not there" :set _set}]
          res     @(pt/exists-batch *c* indices)]
      (is (= [false true true true false] res)))))

(deftest put-get-clj-map
  (let [data {"foo" {"bar" [(rand-int 1000)]}}
        k    (random-key)]
    (is (true? @(pt/create *c* k _set data TTL)))
    (testing "clojure maps can be serialized as-is"
      (let [v @(pt/get-single-no-meta *c* k _set)]
        (is (= data v))                                     ;; per value it is identical
        (is (= PersistentArrayMap (type v)))))))

(deftest put-multiple-bins-get-clj-map
  (let [data {"foo"  {"bar" [(rand-int 1000)]}
              "baz"  true
              "qux"  false
              "quuz" nil}
        k    (random-key)]
    (is (true? @(pt/create *c* k _set data TTL)))
    (testing "clojure maps can be serialized from bins"
      (let [v @(pt/get-single-no-meta *c* k _set)]
        (is (= (get data "foo") (get v "foo")))             ;; per value it is identical
        (is (= (get data "bar") (get v "bar")))             ;; true value returns the same after being sanitized/desanitized
        (is (= (get data "baz") (get v "baz")))             ;; false value returns the same after being sanitized/desanitized
        (is (= (get data "qux") (get v "qux")))             ;; nil value retuns the same after being sanitized/desanitized
        (is (= PersistentArrayMap (type v)))                ;; converted back to a Clojure map instead of HashMap
        (is (true? (map? v)))))))

(deftest get-single-multiple-bins
  (let [data {"foo" [(rand-int 1000)]
              "bar" [(rand-int 1000)]
              "baz" [(rand-int 1000)]}
        k    (random-key)]
    (is (true? @(pt/create *c* k _set data TTL)))
    (testing "bin values can be retrieved individually and all together"
      (let [v1 @(pt/get-single *c* k _set {} ["foo"])
            v2 @(pt/get-single *c* k _set {} ["bar"])
            v3 @(pt/get-single *c* k _set {} ["baz"])
            v4 @(pt/get-single *c* k _set {})]              ;; getting all bins for the record
        (is (= (get data "foo") (get (:payload v1) "foo")))
        (is (= (get data "bar") (get (:payload v2) "bar")))
        (is (= (get data "baz") (get (:payload v3) "baz")))
        (is (= data (:payload v4)))
        (is (true? (map? (:payload v1))))))))

(deftest get-batch-multiple-bins
  (let [data {"foo" [(rand-int 1000)]
              "bar" [(rand-int 1000)]
              "baz" [(rand-int 1000)]}
        k    (random-key)
        k2   (random-key)]
    (is (true? @(pt/create *c* k _set data TTL)))
    (is (true? @(pt/create *c* k2 _set data TTL)))
    (let [brs [{:index k :set _set :bins [:all]}
               {:index k :set _set}
               {:index k2 :set _set :bins ["bar"]}
               {:index "not there" :set _set}]
          res @(pt/get-batch *c* brs)]
      (is (= [data data (select-keys data ["bar"]) nil] (mapv :payload res)))
      (is (= [1 1 1 nil] (mapv :gen res))))))

(deftest adding-bins-to-record
  (let [data     {"foo" [(rand-int 1000)]
                  "bar" [(rand-int 1000)]
                  "baz" [(rand-int 1000)]}
        new-data {"qux" [(rand-int 1000)]}
        k        (random-key)]
    (is (true? @(pt/create *c* k _set data TTL)))
    (is (true? @(pt/add-bins *c* k _set new-data TTL)))     ;; adding value to bin
    (testing "bin values can be added to existing records"
      (let [v @(pt/get-single-no-meta *c* k _set)]
        (is (= v (merge data new-data)))
        (is (= (into #{} (keys v)) #{"foo" "bar" "baz" "qux"}))))
    (testing "adding a bin that already exists in the record with a new value"
      (let [existing-bin {"foo" [(rand-int 1000)]}]
        (is (true? @(pt/add-bins *c* k _set existing-bin TTL)))
        (is (= (get existing-bin "foo")
               (get @(pt/get-single-no-meta *c* k _set) "foo")))))))

(deftest removing-bins-from-record
  (let [data     {"foo" [(rand-int 1000)]
                  "bar" [(rand-int 1000)]
                  "baz" [(rand-int 1000)]
                  "qux" [(rand-int 1000)]}
        bin-keys ["foo" "bar" "baz"]
        k        (random-key)]
    (is (true? @(pt/create *c* k _set data TTL)))
    (is (true? @(pt/delete-bins *c* k _set bin-keys TTL)))  ;; removing value from bin
    (testing "bin values can be removed from existing records"
      (let [v @(pt/get-single-no-meta *c* k _set)]
        (is (= v (apply dissoc data bin-keys)))
        (is (= (keys v) ["qux"]))))))

(deftest update-test
  (let [k (random-key)]
    (is (true? @(pt/create *c* k _set 16 TTL)))
    (is (true? @(pt/update *c* k _set 17 1 TTL)))
    (let [{:keys [payload gen]} @(pt/get-single *c* k _set)]
      (is (= 17 payload))
      (is (= 2 gen)))))

(deftest too-long-bin-name
  (let [long-bin-name "thisstringislongerthan14characters"
        k             (random-key)]
    (is (thrown-with-msg?
          Exception #"Bin names have to be <= 14 characters..."
          @(pt/put *c* k _set {long-bin-name "foo"} TTL)))))

(deftest update-with-wrong-gen
  (let [data (rand-int 1000)
        k    (random-key)]
    (is (true? @(pt/create *c* k _set data TTL)))
    (let [{:keys [payload gen]} @(pt/get-single *c* k _set)]
      (is (= data payload))
      (is (= 1 gen))
      (is (thrown-with-msg? ExecutionException #"Generation error"
                            @(pt/update *c* k _set (inc data) 42 TTL))))))

(deftest put-create-only-raises-exception
  (let [k (random-key)]
    (is (true? @(pt/create *c* k _set 1 TTL)))
    (is (thrown-with-msg? ExecutionException #"Key already exists"
                          @(pt/create *c* k _set 1 TTL)))))

(deftest put-replace-only-raises-exception
  (is (thrown-with-msg? ExecutionException #"Key not found"
                        @(pt/replace-only *c* "not here" _set 1 TTL))))
(deftest delete
  (let [k (random-key)]
    (is (true? @(pt/put *c* k _set 1 TTL)))
    (is (true? @(pt/delete *c* k _set)))
    (is (false? @(pt/delete *c* k _set)))
    (is (nil? @(pt/get-single *c* k _set)))))

(deftest put-multiple
  (let [data [(rand-int 1000) (rand-int 1000)]
        k    (random-key)
        k2   (random-key)]
    (is (= [true true]
           @(pt/put-multiple *c* [k k2] (repeat _set) data (repeat TTL))))
    (let [[{d1 :payload g1 :gen} {d2 :payload g2 :gen} {d3 :payload g3 :gen}]
          @(pt/get-batch *c* [{:index k :set _set}
                              {:index k2 :set _set}
                              {:index "not-there" :set _set}])]
      (is (= d1 (first data)))
      (is (= d2 (second data)))
      (is (= 1 g1 g2))
      (is (nil? d3))
      (is (nil? g3)))))

(deftest get-batch-transcoded
  (let [put-json (fn [k d] (pt/put *c* k _set d TTL {:transcoder json/generate-string}))
        data     [{:a (rand-int 1000)} {:a (rand-int 1000)}]
        k        (random-key)
        k2       (random-key)
        batch    [{:index k :set _set} {:index k2 :set _set}]]
    (is (true? @(put-json k (first data))))
    (is (true? @(put-json k2 (second data))))
    (let [[{d1 :payload g1 :gen}
           {d2 :payload g2 :gen}]
          @(pt/get-batch *c* batch
                         {:transcoder (fn [batch-result]
                                        (mapv
                                          (fn [res]
                                            (update res :payload #(json/parse-string % keyword)))
                                          batch-result))})]
      (is (= d1 (first data)))
      (is (= d2 (second data)))
      (is (= 1 g1 g2)))))

(deftest exists
  (let [k (random-key)]
    (is (true? @(pt/create *c* k _set 1 TTL)))
    (is (true? @(pt/exists? *c* k _set)))
    (is (true? @(pt/delete *c* k _set)))
    (is (false? @(pt/exists? *c* k _set)))))

(deftest touch
  (let [k (random-key)]
    (is (true? @(pt/create *c* k _set 1 TTL)))
    (let [{:keys [ttl]} @(pt/get-single *c* k _set)]
      (is (true? @(pt/touch *c* k _set (inc TTL))))
      (let [{new-ttl :ttl} @(pt/get-single *c* k _set)]
        (is (< ttl new-ttl))))))

(deftest transcoder-failure
  (let [k (random-key)]
    (is (true? @(pt/create *c* k _set 1 TTL)))
    (let [transcoder (fn [_] (throw (Exception. "oh-no")))]
      (is (thrown-with-msg? Exception #"oh-no" @(pt/get-single *c* k _set {:transcoder transcoder}))))))

(def empty-ctx-varargs (into-array CTX []))

(deftest operations-lists
  (let [k       (random-key)
        result1 @(pt/operate *c* k _set TTL
                             [(ListOperation/append
                                (ListPolicy. ListOrder/UNORDERED ListWriteFlags/ADD_UNIQUE)
                                ""
                                (Value/get "foo")
                                empty-ctx-varargs)])
        result2 @(pt/operate *c* k _set TTL
                             [(ListOperation/append
                                (ListPolicy. ListOrder/UNORDERED ListWriteFlags/ADD_UNIQUE)
                                ""
                                (Value/get "bar")
                                empty-ctx-varargs)])]
    (is (thrown-with-msg? ExecutionException
                          #"Map key exists"
                          @(pt/operate *c* k _set TTL
                                       [(ListOperation/append
                                          (ListPolicy. ListOrder/UNORDERED ListWriteFlags/ADD_UNIQUE)
                                          ""
                                          (Value/get "bar")
                                          empty-ctx-varargs)])))
    (let [result3 @(pt/operate *c* k _set TTL
                               [(ListOperation/appendItems
                                  (ListPolicy. ListOrder/UNORDERED (bit-or ListWriteFlags/ADD_UNIQUE
                                                                           ListWriteFlags/PARTIAL
                                                                           ListWriteFlags/NO_FAIL))
                                  ""
                                  [(Value/get "bar") (Value/get "baz")]
                                  empty-ctx-varargs)])
          result4 @(pt/get-single *c* k _set)]
      (is (= 1 (:payload result1)))
      (is (= 2 (:payload result2)))
      (is (= 3 (:payload result3)))
      (is (= ["foo" "bar" "baz"] (vec (:payload result4)))))))

(deftest operations-windowed-uniq-list
  (let [outer-key           (random-key)
        get-all             #(:payload @(pt/get-single *c* outer-key _set))
        key-ctx             (fn [^String k]
                              (into-array CTX [(CTX/mapKey (Value/get k))]))
        outer-ctx           (into-array CTX [])
        outer-map-policy    (MapPolicy. MapOrder/KEY_ORDERED (bit-or MapWriteFlags/CREATE_ONLY
                                                                     MapWriteFlags/NO_FAIL))
        bin-name            ""
        max-entries         4
        initial-empty-value #(hash-map (Value/get %) (Value/get (ArrayList. 1)))
        append              (fn [k v]
                              (first
                                (:payload
                                  @(pt/operate
                                     *c* outer-key _set TTL
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
  (let [k       (random-key)
        result1 @(pt/operate *c* k _set TTL
                             [(MapOperation/put
                                (MapPolicy. MapOrder/UNORDERED MapWriteFlags/DEFAULT)
                                ""
                                (Value/get "foo")
                                (Value/get "foo1")
                                empty-ctx-varargs)])
        result2 @(pt/operate *c* k _set TTL
                             [(MapOperation/put
                                (MapPolicy. MapOrder/UNORDERED MapWriteFlags/DEFAULT)
                                ""
                                (Value/get "bar")
                                (Value/get "bar1")
                                empty-ctx-varargs)])]
    (is (thrown-with-msg? ExecutionException
                          #"Map key exists"
                          @(pt/operate *c* k _set TTL
                                       [(MapOperation/put
                                          (MapPolicy. MapOrder/UNORDERED MapWriteFlags/CREATE_ONLY)
                                          ""
                                          (Value/get "foo")
                                          (Value/get "foo2")
                                          empty-ctx-varargs)])))
    (let [result3 @(pt/operate *c* k _set TTL
                               [(MapOperation/putItems
                                  (MapPolicy. MapOrder/UNORDERED (bit-or MapWriteFlags/CREATE_ONLY
                                                                         MapWriteFlags/PARTIAL
                                                                         MapWriteFlags/NO_FAIL))
                                  ""
                                  {(Value/get "foo") (Value/get "foo2")
                                   (Value/get "baz") (Value/get "baz1")}
                                  empty-ctx-varargs)])
          result4 @(pt/get-single *c* k _set)]
      (is (= 1 (:payload result1)))
      (is (= 2 (:payload result2)))
      (is (= 3 (:payload result3)))
      (is (= {"foo" "foo1" "bar" "bar1" "baz" "baz1"} (into {} (:payload result4)))))))

(deftest operations-sets-maps-based
  (let [k (random-key)]
    (letfn [(set-add [v]
              (pt/operate *c* k _set TTL
                          [(MapOperation/put
                             (MapPolicy. MapOrder/UNORDERED (bit-or
                                                              MapWriteFlags/CREATE_ONLY
                                                              MapWriteFlags/NO_FAIL))
                             ""
                             (Value/get v)
                             (Value/get (byte 1))
                             empty-ctx-varargs)]))
            (set-pop [v]
              (pt/operate *c* k _set TTL
                          [(MapOperation/removeByKey
                             ""
                             (Value/get v)
                             MapReturnType/KEY
                             empty-ctx-varargs)]))
            (set-getall []
              (letfn [(->set [res] (->> ^HashMap (:payload res)
                                        .keySet
                                        (into #{})))]
                @(pt/get-single *c* k _set {:transcoder ->set})))
            (set-size []
              (pt/operate *c* k _set TTL
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
              (pt/operate *c* k _set TTL
                          [(ListOperation/append
                             (ListPolicy. ListOrder/UNORDERED (bit-or ListWriteFlags/ADD_UNIQUE
                                                                      ListWriteFlags/PARTIAL
                                                                      ListWriteFlags/NO_FAIL))
                             ""
                             (Value/get v)
                             empty-ctx-varargs)]))
            (set-pop [v]
              (pt/operate *c* k _set TTL
                          [(ListOperation/removeByValue
                             ""
                             (Value/get v)
                             ListReturnType/VALUE
                             empty-ctx-varargs)]))
            (set-getall []
              (letfn [(->set [res] (->> res :payload (into #{})))]
                @(pt/get-single *c* k _set {:transcoder ->set})))
            (set-size []
              (pt/operate *c* k _set TTL
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

(deftest batch-operate
  (testing "mixed cdt operations on multiple keys"
    (letfn [(create-batch-write-record [list-bin map-bin map-key ^String string-bin k v]
              (let [as-key (pt/create-key k as-namespace _set)]
                (BatchWrite. as-key (utils/v->array Operation [(ListOperation/append list-bin (Value/get (str v)) nil)
                                                               (MapOperation/put (MapPolicy.) map-bin (Value/get map-key) (Value/get (str v)) nil)
                                                               (Operation/put (Bin. string-bin (str v)))]))))
            (create-touch-record [k _v]
              (let [as-key (pt/create-key k as-namespace _set)]
                (BatchWrite. as-key (utils/v->array Operation [(Operation/touch)]))))
            (create-read-batch-record [k]
              {:index k :set _set})]
      (let [list-bin                      "list"
            map-bin                       "map"
            map-key                       "test-key"
            string-bin                    "string"
            ks                            (take 3 (repeatedly random-key))
            expected-write-result-payload {:result-code 0
                                           :payload     {list-bin   1
                                                         map-bin    1
                                                         string-bin nil}}
            expected-read-payloads        (mapv (fn [^String val]
                                                  (hash-map map-bin {map-key (str val)}
                                                            list-bin [(str val)]
                                                            string-bin (str val))) (range 3))
            batch-write-records           (mapv (partial create-batch-write-record
                                                         list-bin
                                                         map-bin
                                                         map-key
                                                         string-bin) ks (range))
            batch-touch-records           (mapv create-touch-record ks (range))
            batch-read-records            (mapv create-read-batch-record ks)]
        (is (every? #(= expected-write-result-payload (select-keys % [:result-code :payload]))
                    @(pt/batch-operate *c* batch-write-records)))
        (is (= expected-read-payloads
               (mapv :payload @(pt/get-batch *c* batch-read-records))))
        (is (every? #(zero? (:result-code %)) @(pt/batch-operate *c* batch-touch-records)))))))

(deftest default-read-policy
  (let [rp (.getReadPolicyDefault ^AerospikeClient (.-client ^SimpleAerospikeClient *c*))]
    (is (= Replica/SEQUENCE (.replica rp)))                 ;; Try node containing master partition first.
    ;; If connection fails, all commands try nodes containing replicated partitions.
    ;; If socketTimeout is reached, reads also try nodes containing replicated partitions,
    ;; but writes remain on master node.)))
    ;; This option requires ClientPolicy.requestProleReplicas to be enabled in order to function properly.
    (is (= 30000 (.socketTimeout rp)))                      ;; 30 seconds default
    (is (= 1000 (.totalTimeout rp)))                        ;; total timeout of 1 second
    (is (= 3000 (.timeoutDelay rp)))                        ;; no delay, connection closed on timeout
    (is (= 2 (.maxRetries rp)))                             ;; initial attempt + 2 retries = 3 attempts
    (is (zero? (.sleepBetweenRetries rp)))                  ;; do not sleep between retries
    (is (false? (.sendKey rp)))                             ;; do not send the user defined key
    (is (= ReadModeSC/SESSION (.readModeSC rp)))))          ;; Ensures this client will only see an increasing sequence of record versions. Server only reads from master. This is the default.

(deftest configure-read-and-batch-policy
  (let [c  (client/init-simple-aerospike-client
             *as-hosts* as-namespace
             {"readPolicyDefault"  (policy/map->policy {"ReadModeAP"          "ALL"
                                                        "ReadModeSC"          "LINEARIZE"
                                                        "maxRetries"          1
                                                        "Replica"             "RANDOM"
                                                        "sendKey"             true
                                                        "sleepBetweenRetries" 100
                                                        "socketTimeout"       1000
                                                        "timeoutDelay"        2000
                                                        "totalTimeout"        3000})
              "batchPolicyDefault" (policy/map->batch-policy {"allowInline"          false
                                                              "maxConcurrentThreads" 2
                                                              "sendSetName"          true})})


        rp ^Policy (.getReadPolicyDefault ^AerospikeClient (.-client ^SimpleAerospikeClient c))
        bp ^BatchPolicy (.getBatchPolicyDefault ^AerospikeClient (.-client ^SimpleAerospikeClient c))]
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
  (let [rp ^WritePolicy (.getWritePolicyDefault ^AerospikeClient (.-client ^SimpleAerospikeClient *c*))]
    (is (= ReadModeAP/ONE (.readModeAP rp)))                ;; Involve master only in the read operation.
    (is (= Replica/SEQUENCE (.replica rp)))                 ;; Try node containing master partition first.
    ;; If connection fails, all commands try nodes containing replicated partitions.
    ;; If socketTimeout is reached, reads also try nodes containing replicated partitions,
    ;; but writes remain on master node.)))
    ;; This option requires ClientPolicy.requestProleReplicas to be enabled in order to function properly.
    ;; This option requires ClientPolicy.requestProleReplicas to be enabled in order to function properly.
    (is (= 30000 (.socketTimeout rp)))                      ;; 30 seconds default
    (is (= 1000 (.totalTimeout rp)))                        ;; total timeout of 1 second
    (is (= 3000 (.timeoutDelay rp)))                        ;; no delay, connection closed on timeout
    (is (= 2 (.maxRetries rp)))                             ;; initial attempt + 2 retries = 3 attempts
    (is (zero? (.sleepBetweenRetries rp)))                  ;; do not sleep between retries
    (is (false? (.sendKey rp)))                             ;; do not send the user defined key
    (is (= ReadModeSC/SESSION (.readModeSC rp)))))          ;; Ensures this client will only see an increasing sequence of record versions. Server only reads from master. This is the default.

(deftest configure-write-policy
  (let [c  (client/init-simple-aerospike-client
             *as-hosts* as-namespace
             {"writePolicyDefault" (policy/map->write-policy {"CommitLevel"        "COMMIT_MASTER"
                                                              "durableDelete"      true
                                                              "expiration"         1000
                                                              "generation"         7
                                                              "GenerationPolicy"   "EXPECT_GEN_GT"
                                                              "RecordExistsAction" "REPLACE_ONLY"
                                                              "respondAllOps"      true})})

        wp ^WritePolicy (.getWritePolicyDefault ^AerospikeClient (.-client ^SimpleAerospikeClient c))]
    (is (= ReadModeAP/ONE (.readModeAP wp)))
    (is (true? (.durableDelete wp)))
    (is (= 1000 (.expiration wp)))
    (is (= 7 (.generation wp)))
    (is (= GenerationPolicy/EXPECT_GEN_GT (.generationPolicy wp)))
    (is (= RecordExistsAction/REPLACE_ONLY (.recordExistsAction wp)))
    (is (true? (.respondAllOps wp)))))

(deftest configure-batch-write-policies
  (let [expression                (Exp/build (Exp/ge (Exp/intBin "a") (Exp/intBin "b")))
        c                         (client/init-simple-aerospike-client
                                    *as-hosts* as-namespace
                                    {"batchWritePolicyDefault"       (policy/map->batch-write-policy {"CommitLevel"        "COMMIT_MASTER"
                                                                                                      "durableDelete"      true
                                                                                                      "expiration"         1000
                                                                                                      "generation"         7
                                                                                                      "GenerationPolicy"   "EXPECT_GEN_GT"
                                                                                                      "RecordExistsAction" "REPLACE_ONLY"
                                                                                                      "filterExp"          expression})
                                     "batchParentPolicyWriteDefault" (policy/map->batch-policy {"allowInline"          false
                                                                                                "maxConcurrentThreads" 2
                                                                                                "sendSetName"          true})})

        batch-write-policy        ^BatchWritePolicy (.getBatchWritePolicyDefault ^AerospikeClient (.-client ^SimpleAerospikeClient c))
        batch-parent-write-policy ^BatchPolicy (.getBatchParentPolicyWriteDefault ^AerospikeClient (.-client ^SimpleAerospikeClient c))]
    (is (true? (.durableDelete batch-write-policy)))
    (is (= 1000 (.expiration batch-write-policy)))
    (is (= 7 (.generation batch-write-policy)))
    (is (= GenerationPolicy/EXPECT_GEN_GT (.generationPolicy batch-write-policy)))
    (is (= RecordExistsAction/REPLACE_ONLY (.recordExistsAction batch-write-policy)))
    (is (= expression (.filterExp batch-write-policy)))
    (is (= CommitLevel/COMMIT_MASTER (.commitLevel batch-write-policy)))

    (is (false? (.allowInline batch-parent-write-policy)))
    (is (= 2 (.maxConcurrentThreads batch-parent-write-policy)))
    (is (true? (.sendSetName batch-parent-write-policy)))))

(deftest set-entry
  (let [data        (rand-int 1000)
        update-data (rand-int 1000)
        k           (random-key)]
    (is (true? @(pt/set-single *c* k _set data TTL)))
    (is (= data @(pt/get-single-no-meta *c* k _set)))
    (is (true? @(pt/set-single *c* k _set update-data TTL)))
    (is (= update-data @(pt/get-single-no-meta *c* k _set)))))

(deftest scan-test
  (let [conf           {:policy (policy/map->write-policy {"sendKey" true})}
        aero-namespace "test"
        ttl            5
        k              (random-key)
        k2             (random-key)
        k3             (random-key)
        ks             #{k k2 k3}
        delete-records (fn []
                         @(pt/delete *c* k _set)
                         @(pt/delete *c* k2 _set)
                         @(pt/delete *c* k3 _set))]

    (testing "it should throw an IllegalArgumentException when:
             conf is missing, :callback is missing, or :callback is not a function"
      (is (thrown? IllegalArgumentException
                   @(pt/scan-set *c* aero-namespace _set nil)))
      (is (thrown? IllegalArgumentException
                   @(pt/scan-set *c* aero-namespace _set {})))
      (is (thrown? IllegalArgumentException
                   @(pt/scan-set *c* aero-namespace _set {:callback "not a function"}))))

    (testing "it should throw a ClassCastException when :bins is not a vector"
      (is (thrown? ClassCastException
                   @(pt/scan-set *c* aero-namespace _set {:callback (constantly true) :bins {}}))))

    (testing "it should return all the items in the set"
      @(pt/put-multiple *c* [k k2 k3] (repeat _set) [10 20 30] (repeat ttl) conf)

      (let [res      (atom [])
            callback (fn [k v]
                       (when (ks (str k))
                         (swap! res conj [(.toString ^Value k) (:payload v)])))]

        @(pt/scan-set *c* aero-namespace _set {:callback callback})
        (is (= (sort-by first @res)
               (sort-by first [[k 10] [k2 20] [k3 30]]))))

      (delete-records))

    (testing "it should return only the bins that were requested"
      (let [data [{"name" "John" "occupation" "Carpenter"}
                  {"name" "Jerry" "occupation" "Bus Driver"}
                  {"name" "Jack" "occupation" "Chef"}]]

        @(pt/put-multiple *c* [k k2 k3] (repeat _set) data (repeat ttl) conf)

        (let [res      (atom [])
              callback (fn [k v]
                         (when (ks (str k))
                           (swap! res conj [(.toString ^Value k) (:payload v)])))]

          @(pt/scan-set *c* aero-namespace _set {:callback callback :bins ["occupation"]})

          (is (= (sort-by first @res)
                 (sort-by first
                          [[k {"occupation" "Carpenter"}]
                           [k2 {"occupation" "Bus Driver"}]
                           [k3 {"occupation" "Chef"}]]))))
        (delete-records)))

    (testing "it can update items during a scan"
      (let [client   *c*
            callback (fn [k v]
                       (when (ks (str k))
                         (pt/put client (.toString ^Value k) _set (inc (:payload v)) ttl)))]

        @(pt/put-multiple *c* [k k2 k3] (repeat _set) [10 20 30] (repeat ttl) conf)

        @(pt/scan-set *c* aero-namespace _set {:callback callback})
        (Thread/sleep 50)                                   ;wait for callback completion
        (let [res @(pt/get-batch *c* [{:index k :set _set}
                                      {:index k2 :set _set}
                                      {:index k3 :set _set}])]
          (is (= (mapv :payload res) [11 21 31]))))
      (delete-records))

    (testing "it can delete items during a scan"
      (let [client   *c*
            callback (fn [k _]
                       (when (ks (str k))
                         (pt/delete client (.toString ^Value k) _set)))]

        @(pt/put-multiple *c* [k k2 k3] (repeat _set) [10 20 30] (repeat ttl) conf)

        @(pt/scan-set *c* aero-namespace _set {:callback callback})

        (is (empty? (filter :payload @(pt/get-batch *c* [{:index k :set _set}
                                                         {:index k2 :set _set}
                                                         {:index k3 :set _set}])))))
      (delete-records))

    (testing "it should stop the scan when the callback returns :abort-scan"
      @(pt/put-multiple *c* [k k2 k3] (repeat _set) [10 20 30] (repeat ttl) conf)

      (let [res      (atom [])
            counter  (atom 0)
            callback (fn [k v]
                       (when (ks (str k))
                         (if (< (swap! counter inc) 2)
                           (swap! res conj [(.toString ^Value k) (:payload v)])
                           :abort-scan)))]

        (is (false? @(pt/scan-set *c* aero-namespace _set {:callback callback})))
        (is (= 1 (count @res))))

      (delete-records))))

(deftest client-events-test
  (let [success-spy               (spy/spy)
        failure-spy               (spy/spy)
        create-mock-client-events (fn [on-success-spy on-failure-spy]
                                    (reify pt/ClientEvents
                                      (on-success [_this op-name op-result index op-start-time]
                                        (on-success-spy op-name op-result index op-start-time)
                                        op-result)
                                      (on-failure [_this op-name op-ex index op-start-time]
                                        (on-failure-spy op-name op-ex index op-start-time)
                                        op-ex)))
        client-events             (create-mock-client-events success-spy failure-spy)
        c                         (client/init-simple-aerospike-client *as-hosts* as-namespace {:client-events client-events})
        index                     (random-key)
        payload                   {"string" "hello"}]
    (letfn [(nth-args-map [spy-instance ks n]
              (let [args-map (zipmap ks (spy/nth-call spy-instance n))]
                (assert number? (:op-start-time args-map))
                (dissoc args-map :op-start-time)))
            (success-nth-args-map [spy-instance n]
              (nth-args-map spy-instance [:op-name :op-result :index :op-start-time] n))
            (failure-nth-args-map [spy-instance n]
              (nth-args-map spy-instance [:op-name :op-ex :index :op-start-time] n))]
      (testing "client events is called with the right parameters using default client-events passed on client initialization"
        @(pt/create c index _set payload TTL)
        @(pt/get-single c index _set)
        (is (thrown-with-msg? ExecutionException #"Generation error" @(pt/update c index _set "new-payload" 42 TTL)))

        (is (= {:op-name   :write
                :op-result true
                :index     index} (success-nth-args-map success-spy 0)))
        (is (= {:op-name   :read
                :op-result payload
                :index     index} (-> (success-nth-args-map success-spy 1)
                                      (update :op-result :payload))))
        (is (= {:op-name :write
                :op-ex   3
                :index   index} (-> (failure-nth-args-map failure-spy 0)
                                    (update :op-ex #(.getResultCode %))))))
      (testing "client events is called with the right parameters using custom client-events per op"
        (let [custom-success-spy   (spy/spy)
              custom-failure-spy   (spy/spy)
              custom-client-events (create-mock-client-events custom-success-spy custom-failure-spy)]
          @(pt/put c index _set payload TTL {:client-events [custom-client-events]})
          @(pt/get-single c index _set {:client-events [custom-client-events]})
          (is (thrown-with-msg? ExecutionException #"Generation error" @(pt/update c index _set "new-payload" 42 TTL {:client-events [custom-client-events]})))
          (is (= {:op-name   :write
                  :op-result true
                  :index     index} (success-nth-args-map custom-success-spy 0)))
          (is (= {:op-name   :read
                  :op-result payload
                  :index     index} (-> (success-nth-args-map custom-success-spy 1)
                                        (update :op-result :payload))))
          (is (= {:op-name :write
                  :op-ex   3
                  :index   index} (-> (failure-nth-args-map custom-failure-spy 0)
                                      (update :op-ex #(.getResultCode %))))))))))
