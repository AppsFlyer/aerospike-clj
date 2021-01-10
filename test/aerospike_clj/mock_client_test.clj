(ns aerospike-clj.mock-client-test
  (:refer-clojure :exclude [update])
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [aerospike-clj.client :as c]
            [aerospike-clj.mock-client :as mock]
            [aerospike-clj.protocols :as pt]
            [clojure.string])
  (:import [com.aerospike.client ResultCode]
           [java.util.concurrent ExecutionException]
           [aerospike_clj.client SimpleAerospikeClient]))

(def ^:dynamic ^SimpleAerospikeClient client nil)

(defn- rebind-client-to-mock [test-fn]
  (binding [client (mock/create-instance)]
    (test-fn)))

(use-fixtures :each rebind-client-to-mock)

(deftest get-single-test
  (pt/put client "foo" "my-set" {"bar" 20} 86400)

  (testing "it should return a deferrable of a single record if it exists"
    (let [expected {:payload {"bar" 20} :ttl 86400 :gen 1}
          actual   (pt/get-single client "foo" "my-set")]
      (is (= @actual expected))))

  (testing "it should return a deferrable of nil if it doesn't exist"
    (let [actual (pt/get-single client "foo" nil)]
      (is (= @actual nil)))
    (let [actual (pt/get-single client "does-not-exist" "my-set")]
      (is (= @actual nil)))))

(deftest get-single-no-meta-test
  (pt/put client "foo" nil "bar" 30)

  (testing "it should return a deferrable of a single record without metadata if it exists"
    (let [expected "bar"
          actual   (pt/get-single-no-meta client "foo" nil)]
      (is (= @actual expected))))

  (testing "it should return a deferrable of nil if it doesn't exist"
    (let [actual (pt/get-single-no-meta client "does-not-exist" nil)]
      (is (= @actual nil)))))

(deftest get-multiple-test
  (testing "it should return a deferrable of vector of matched records"
    (pt/put-multiple client ["foo" "bar"] [nil "some-set"] ["is best" "is better"] [86400 43200])

    (let [expected [{:payload "is best" :ttl 86400 :gen 1}]
          actual   (pt/get-multiple client ["foo"] [nil])]
      (is (= @actual expected)))

    (let [expected [{:payload "is better" :ttl 43200 :gen 1}]
          actual   (pt/get-multiple client ["bar"] ["some-set"])]
      (is (= @actual expected)))

    (let [expected [{:payload "is best" :ttl 86400 :gen 1}
                    {:payload "is better" :ttl 43200 :gen 1}]
          actual   (pt/get-multiple client ["foo" "bar"] [nil "some-set"])]
      (is (= @actual expected)))

    (let [expected [nil nil]
          actual   (pt/get-multiple client ["does-not-exist" "also-not-here"] [nil "some-set"])]
      (is (= @actual expected)))))

(deftest get-batch-test
  (testing "it should return a deferrable of vector of matched records"
    (pt/put-multiple client
                     ["foo" "bar" "fuzz"]
                     [nil "some-set" "some-set"]
                     ["is best" "is better" {"rank" "not so good" "solution" "give up"}]
                     [86400 43200 10])

    (let [expected [{:payload "is best" :ttl 86400 :gen 1} nil]
          actual   (pt/get-batch client [{:index "foo" :set nil} {:index "bar" :set "bar"}])]
      (is (= @actual expected)))

    (let [expected [{:payload "is better" :ttl 43200 :gen 1} nil {:payload {"rank" "not so good"} :ttl 10 :gen 1}]
          actual   (pt/get-batch client [{:index "bar" :set "some-set"} {:index "bar" :set nil} {:index "fuzz" :set "some-set" :bins ["rank"]}])]
      (is (= @actual expected)))))

(deftest exists?-test
  (testing "it should return a deferrable of boolean indicating whether a record exists"
    (pt/put client "foo" nil "bar" 30)
    (is (true? @(pt/exists? client "foo" nil)))
    (is (false? @(pt/exists? client "bar" nil)))
    (is (false? @(pt/exists? client "FOO" nil)))
    (is (false? @(pt/exists? client "" nil)))
    (is (false? @(pt/exists? client nil nil)))))

(deftest exists-batch-test
  (testing "it should return a deferrable vector of booleans indicating whether each key exists"
    (pt/put-multiple client
                     ["foo" "fuzz" "bar" "baz"]
                     ["set1" "set2" "set1" "set3"]
                     [1 1 1 1]
                     [30 30 30 30])

    (is (= @(pt/exists-batch client [{:index "foo" :set "set1"}
                                     {:index "fuzz" :set "set2"}
                                     {:index "bar" :set "set1"}
                                     {:index "baz" :set "set3"}])
           [true true true true]))

    (is (= @(pt/exists-batch client [{:index "foo" :set "set1"}
                                     {:index "fuzz" :set "set1"}
                                     {:index "bar" :set "set1"}
                                     {:index "baz" :set "set1"}])
           [true false true false]))

    (is (= @(pt/exists-batch client [{:index "zoo" :set "set1"}
                                     {:index "fizz" :set "set1"}
                                     {:index "vec" :set "set1"}
                                     {:index "pez" :set "set1"}])
           [false false false false]))))

(deftest put-test
  (testing "it should create a new record if it doesn't exist"
    (pt/put client "foo" nil "bar" 30)
    (let [expected {:payload "bar" :ttl 30 :gen 1}
          actual   (pt/get-single client "foo" nil)]
      (is (= @actual expected))))

  (testing "it should replace a record if it exists"
    (pt/put client "foo" nil "bar" 30)
    (let [expected {:payload "bar" :ttl 30 :gen 1}
          actual   (pt/get-single client "foo" nil)]
      (is (= @actual expected)))

    (pt/put client "foo" nil "baz" 40)
    (let [expected {:payload "baz" :ttl 40 :gen 1}
          actual   (pt/get-single client "foo" nil)]
      (is (= @actual expected)))))

(deftest set-single-test
  (testing "it should create a new record if it doesn't exist"
    (pt/set-single client "foo" nil "bar" 30)
    (let [expected {:payload "bar" :ttl 30 :gen 1}
          actual   (pt/get-single client "foo" nil)]
      (is (= @actual expected))))

  (testing "it should update a record if it exists"
    (pt/set-single client "foo" nil "baz" 30)
    (let [expected {:payload "baz" :ttl 30 :gen 2}
          actual   (pt/get-single client "foo" nil)]
      (is (= @actual expected)))

    (pt/set-single client "foo" nil "baz" 40)
    (let [expected {:payload "baz" :ttl 40 :gen 3}
          actual   (pt/get-single client "foo" nil)]
      (is (= @actual expected)))))

(deftest put-multiple-test
  (testing "it should create a new record for each one that doesn't exist and replace each one
  that does"
    (let [put-result (pt/put-multiple client ["foo" "bar"] [nil nil] [1 2] [60 60])
          expected   [{:payload 1 :ttl 60 :gen 1}
                      {:payload 2 :ttl 60 :gen 1}]
          actual     (pt/get-multiple client ["foo" "bar"] [nil nil])]
      (is (= @put-result [true true]))
      (is (= @actual expected)))


    (let [put-result (pt/put-multiple client ["foo" "baz"] [nil nil] [3 4] [120 60])
          expected   [{:payload 3 :ttl 120 :gen 1}
                      {:payload 2 :ttl 60 :gen 1}
                      {:payload 4 :ttl 60 :gen 1}]
          actual     (pt/get-multiple client ["foo" "bar" "baz"] [nil nil nil])]
      (is (= @put-result [true true]))
      (is (= @actual expected)))))

(defn get-ex-code [ex]
  (-> ex .getCause .getResultCode))

(deftest add-bins-test
  (testing "it should add bins to an existing record without modifying old data"
    (pt/set-single client "person" nil {"fname" "John" "lname" "Baker"} 30)
    (pt/add-bins client "person" nil {"prefix" "Mr."} 60)

    (let [expected {:payload {"fname" "John" "lname" "Baker" "prefix" "Mr."} :ttl 60 :gen 1}
          actual   (pt/get-single client "person" nil)]
      (is (= @actual expected))))

  (testing "it should throw an exception if key doesn't exist"
    (try
      @(pt/add-bins client "does-not-exist" nil {"prefix" "Mr."} 60)
      (throw (AssertionError. "Expected AerospikeException to be thrown in `add-bins-test`"))
      (catch ExecutionException ex
        (is (= (get-ex-code ex) ResultCode/KEY_NOT_FOUND_ERROR))))))

(deftest create-test
  (testing "it should create a new record"
    (pt/create client "foo" "set1" "bar" 60)
    (pt/create client "baz" nil {"value" 1} 30)

    (let [expected {:payload "bar" :ttl 60 :gen 1}
          actual   (pt/get-single client "foo" "set1")]
      (is (= @actual expected)))

    (let [expected {:payload {"value" 1} :ttl 30 :gen 1}
          actual   (pt/get-single client "baz" nil)]
      (is (= @actual expected))))

  (testing "it should throw an exception when a key already exists"
    (doseq [args [["key1" "set1" "val1" 60] ["key2" nil "val2" 60]]]
      (apply pt/create client args)
      (try
        @(apply pt/create client args)
        (throw (AssertionError.
                 (str "Expected AerospikeException to be thrown in `create-test`: " args)))
        (catch ExecutionException ex
          (is (= (get-ex-code ex) ResultCode/KEY_EXISTS_ERROR)))))))

(deftest touch-test
  (testing "it should update the record's ttl if it exists"
    (pt/create client "foo" "set1" "bar" 60)
    (pt/touch client "foo" "set1" 10)

    (let [expected {:payload "bar" :ttl 10 :gen 1}
          actual   (pt/get-single client "foo" "set1")]
      (is (= @actual expected))))

  (testing "it should throw an exception if a key doesn't exist"
    (try
      @(pt/touch client "does-not-exist" nil 120)
      (throw (AssertionError. "Expected AerospikeException to be thrown in `touch-test`"))
      (catch ExecutionException ex
        (is (= (get-ex-code ex) ResultCode/KEY_NOT_FOUND_ERROR))))))

(deftest delete-test
  (testing "it should delete a record if it exists"
    (pt/create client "foo" "set1" "bar" 60)
    (is (= @(pt/delete client "foo" "set1") true))
    (is (= @(pt/get-single client "foo" "set1") nil))

    (pt/create client "foo" nil "bar" 30)
    (is (= @(pt/delete client "foo" nil) true))
    (is (= @(pt/get-single client "foo" nil) nil)))

  (testing "it should return false if a key doesn't exist"
    (pt/create client "foo" nil "bar" 60)
    (pt/create client "foo" "set1" "bar" 60)
    (let [default-set (mock/get-state client)
          set1        (mock/get-state client "set1")]
      (is (= @(pt/delete client "does-not-exist" nil) false))
      (is (= @(pt/delete client "does-not-exist" "foo-bar") false))

      (is (= default-set (mock/get-state client)))
      (is (= set1 (mock/get-state client "set1"))))))

(deftest delete-bins-test
  (testing "it should delete the given bins in an existing record without modifying others"
    (pt/create client "person" nil {"fname" "John" "lname" "Baker" "prefix" "Mr." "age" 74} 30)
    (pt/delete-bins client "person" nil ["prefix" "age"] 60)

    (let [expected {:payload {"fname" "John" "lname" "Baker"} :ttl 60 :gen 1}
          actual   (pt/get-single client "person" nil)]
      (is (= @actual expected)))

    (pt/delete-bins client "person" nil ["prefix" "age"] 10)
    (let [expected {:payload {"fname" "John" "lname" "Baker"} :ttl 10 :gen 1}
          actual   (pt/get-single client "person" nil)]
      (is (= @actual expected))))

  (testing "it should throw an exception if key doesn't exist"
    (try
      @(pt/delete-bins client "does-not-exist" nil ["fname"] 60)
      (throw (AssertionError. "Expected AerospikeException to be thrown in `delete-bins-test`"))
      (catch ExecutionException ex
        (is (= (get-ex-code ex) ResultCode/KEY_NOT_FOUND_ERROR))))))

(deftest expiry-unix-test
  (testing "it should proxy to aerospike-clj"
    (is (= (c/expiry-unix 100) (c/expiry-unix 100)))))

(deftest operate-test
  (testing "function is not implemented"
    (try
      (pt/operate client "foo" nil 120 [])
      (throw (AssertionError. "Expected RuntimeException to be thrown in `operate-test`"))
      (catch RuntimeException _
        (is (= 1 1))))))

(deftest scan-set-test
  (let [test-data [["John Barker" {"fname" "John" "lname" "Baker" "prefix" "Mr." "age" 74}]
                   ["Fred Marshall" {"fname" "Fred" "lname" "Marshall" "prefix" "Mr." "age" 45}]
                   ["Gina Benson" {"fname" "Gina" "lname" "Benson" "prefix" "Mrs." "age" 39}]]]

    (doseq [v test-data]
      (pt/create client (first v) "people" (second v) 30))

    (testing "it should scan the set and call the user supplied callback for each record"
      (let [res      (atom [])
            callback (fn [k v] (swap! res conj [k (get (:payload v) "age")]))]
        (is (true? @(pt/scan-set client nil "people" {:callback callback})))
        (is (= [["John Barker" 74] ["Fred Marshall" 45] ["Gina Benson" 39]] @res))))

    (testing "it should abort the scan and return false when the callback returns :abort-scan"
      (let [callback (fn [_ v] (when (= (get (:payload v) "age") 45) :abort-scan))]
        (is (false? @(pt/scan-set client nil "people" {:callback callback})))))

    (testing "it should include only the bins requested by the user"
      (let [res      (atom [])
            callback (fn [k v] (swap! res conj [k (:payload v)]))]
        (is (true? @(pt/scan-set client nil "people" {:bins ["age"] :callback callback})))
        (is (= [["John Barker" {"age" 74}] ["Fred Marshall" {"age" 45}] ["Gina Benson" {"age" 39}]] @res))))

    (testing "it should scan the given namespace"
      (let [res      (atom [])
            callback (fn [_ v] (swap! res conj v))]
        (is (true? @(pt/scan-set client nil "children" {:callback callback})))
        (is (empty @res))))))

(deftest healthy-test
  (is (= (pt/healthy? client 0) true)))

(deftest get-cluster-stats-test
  (is (= [[]] (pt/get-cluster-stats client))))

(deftest apply-transcoder-test
  (testing "it should apply the transcoder function to the payload before inserting it"
    (let [conf     {:transcoder #(clojure.core/update % "bar" inc)}
          expected {:payload {"bar" 21} :ttl 86400 :gen 1}]

      (pt/put client "foo" "my-set" {"bar" 20} 86400 conf)
      (is (= @(pt/get-single client "foo" "my-set") expected))))

  (testing "it should apply the transcoder function to the whole record before returning it"
    (let [conf     {:transcoder #(clojure.core/update-in % [:payload "bar"] inc)}
          expected {:payload {"bar" 22} :ttl 86400 :gen 1}
          actual   (pt/get-single client "foo" "my-set" conf)]
      (is (= @actual expected))))

  (testing "it should apply the transcoder function on each record before returning them"
    (pt/put-multiple client ["a" "b" "c"] [nil nil nil] ["d" "e" "f"] [10 20 30])

    (let [upper-case-value #(clojure.core/update % :payload clojure.string/upper-case)
          increment-ttl    #(clojure.core/update % :ttl inc)
          conf             {:transcoder (comp increment-ttl upper-case-value)}
          expected         [{:payload "D" :ttl 11 :gen 1}
                            {:payload "E" :ttl 21 :gen 1}
                            {:payload "F" :ttl 31 :gen 1}]
          actual           @(pt/get-multiple client ["a" "b" "c"] [nil nil nil] conf)]
      (is (= actual expected)))))
