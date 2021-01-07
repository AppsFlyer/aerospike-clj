(ns aerospike-clj.mock-client-test
  (:refer-clojure :exclude [update])
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [aerospike-clj.mock-client :as mock]
            [aerospike-clj.client :as c]
            [clojure.string])
  (:import [com.aerospike.client ResultCode]
           [java.util.concurrent ExecutionException]))

(def ^:dynamic ^c/IAerospikeClient2 client nil)

(defn- rebind-client-to-mock [test-fn]
  (binding [client (mock/create-instance)]
    (test-fn)))

(use-fixtures :each rebind-client-to-mock)

(deftest get-single-test
  (c/put client "foo" "my-set" {"bar" 20} 86400)

  (testing "it should return a deferrable of a single record if it exists"
    (let [expected {:payload {"bar" 20} :ttl 86400 :gen 1}
          actual   (c/get-single client "foo" "my-set")]
      (is (= @actual expected))))

  (testing "it should return a deferrable of nil if it doesn't exist"
    (let [actual (c/get-single client "foo" nil)]
      (is (= @actual nil)))
    (let [actual (c/get-single client "does-not-exist" "my-set")]
      (is (= @actual nil)))))

(deftest get-single-no-meta-test
  (c/put client "foo" nil "bar" 30)

  (testing "it should return a deferrable of a single record without metadata if it exists"
    (let [expected "bar"
          actual   (c/get-single-no-meta client "foo" nil)]
      (is (= @actual expected))))

  (testing "it should return a deferrable of nil if it doesn't exist"
    (let [actual (c/get-single-no-meta client "does-not-exist" nil)]
      (is (= @actual nil)))))

(deftest get-multiple-test
  (testing "it should return a deferrable of vector of matched records"
    (c/put-multiple client ["foo" "bar"] [nil "some-set"] ["is best" "is better"] [86400 43200])

    (let [expected [{:payload "is best" :ttl 86400 :gen 1}]
          actual   (c/get-multiple client ["foo"] [nil])]
      (is (= @actual expected)))

    (let [expected [{:payload "is better" :ttl 43200 :gen 1}]
          actual   (c/get-multiple client ["bar"] ["some-set"])]
      (is (= @actual expected)))

    (let [expected [{:payload "is best" :ttl 86400 :gen 1}
                    {:payload "is better" :ttl 43200 :gen 1}]
          actual   (c/get-multiple client ["foo" "bar"] [nil "some-set"])]
      (is (= @actual expected)))

    (let [expected [nil nil]
          actual   (c/get-multiple client ["does-not-exist" "also-not-here"] [nil "some-set"])]
      (is (= @actual expected)))))

(deftest get-batch-test
  (testing "it should return a deferrable of vector of matched records"
    (c/put-multiple client
                    ["foo" "bar" "fuzz"]
                    [nil "some-set" "some-set"]
                    ["is best" "is better" {"rank" "not so good" "solution" "give up"}]
                    [86400 43200 10])

    (let [expected [{:payload "is best" :ttl 86400 :gen 1} nil]
          actual   (c/get-batch client [{:index "foo" :set nil} {:index "bar" :set "bar"}])]
      (is (= @actual expected)))

    (let [expected [{:payload "is better" :ttl 43200 :gen 1} nil {:payload {"rank" "not so good"} :ttl 10 :gen 1}]
          actual   (c/get-batch client [{:index "bar" :set "some-set"} {:index "bar" :set nil} {:index "fuzz" :set "some-set" :bins ["rank"]}])]
      (is (= @actual expected)))))

(deftest exists?-test
  (testing "it should return a deferrable of boolean indicating whether a record exists"
    (c/put client "foo" nil "bar" 30)
    (is (true? @(c/exists? client "foo" nil)))
    (is (false? @(c/exists? client "bar" nil)))
    (is (false? @(c/exists? client "FOO" nil)))
    (is (false? @(c/exists? client "" nil)))
    (is (false? @(c/exists? client nil nil)))))

(deftest exists-batch-test
  (testing "it should return a deferrable vector of booleans indicating whether each key exists"
    (c/put-multiple client
                    ["foo" "fuzz" "bar" "baz"]
                    ["set1" "set2" "set1" "set3"]
                    [1 1 1 1]
                    [30 30 30 30])

    (is (= @(c/exists-batch client [{:index "foo" :set "set1"}
                                         {:index "fuzz" :set "set2"}
                                         {:index "bar" :set "set1"}
                                         {:index "baz" :set "set3"}])
           [true true true true]))

    (is (= @(c/exists-batch client [{:index "foo" :set "set1"}
                                         {:index "fuzz" :set "set1"}
                                         {:index "bar" :set "set1"}
                                         {:index "baz" :set "set1"}])
           [true false true false]))

    (is (= @(c/exists-batch client [{:index "zoo" :set "set1"}
                                         {:index "fizz" :set "set1"}
                                         {:index "vec" :set "set1"}
                                         {:index "pez" :set "set1"}])
           [false false false false]))))

(deftest put-test
  (testing "it should create a new record if it doesn't exist"
    (c/put client "foo" nil "bar" 30)
    (let [expected {:payload "bar" :ttl 30 :gen 1}
          actual   (c/get-single client "foo" nil)]
      (is (= @actual expected))))

  (testing "it should replace a record if it exists"
    (c/put client "foo" nil "bar" 30)
    (let [expected {:payload "bar" :ttl 30 :gen 1}
          actual   (c/get-single client "foo" nil)]
      (is (= @actual expected)))

    (c/put client "foo" nil "baz" 40)
    (let [expected {:payload "baz" :ttl 40 :gen 1}
          actual   (c/get-single client "foo" nil)]
      (is (= @actual expected)))))

(deftest set-single-test
  (testing "it should create a new record if it doesn't exist"
    (c/set-single client "foo" nil "bar" 30)
    (let [expected {:payload "bar" :ttl 30 :gen 1}
          actual   (c/get-single client "foo" nil)]
      (is (= @actual expected))))

  (testing "it should update a record if it exists"
    (c/set-single client "foo" nil "baz" 30)
    (let [expected {:payload "baz" :ttl 30 :gen 2}
          actual   (c/get-single client "foo" nil)]
      (is (= @actual expected)))

    (c/set-single client "foo" nil "baz" 40)
    (let [expected {:payload "baz" :ttl 40 :gen 3}
          actual   (c/get-single client "foo" nil)]
      (is (= @actual expected)))))

(deftest put-multiple-test
  (testing "it should create a new record for each one that doesn't exist and replace each one
  that does"
    (let [put-result (c/put-multiple client ["foo" "bar"] [nil nil] [1 2] [60 60])
          expected   [{:payload 1 :ttl 60 :gen 1}
                      {:payload 2 :ttl 60 :gen 1}]
          actual     (c/get-multiple client ["foo" "bar"] [nil nil])]
      (is (= @put-result [true true]))
      (is (= @actual expected)))


    (let [put-result (c/put-multiple client ["foo" "baz"] [nil nil] [3 4] [120 60])
          expected   [{:payload 3 :ttl 120 :gen 1}
                      {:payload 2 :ttl 60 :gen 1}
                      {:payload 4 :ttl 60 :gen 1}]
          actual     (c/get-multiple client ["foo" "bar" "baz"] [nil nil nil])]
      (is (= @put-result [true true]))
      (is (= @actual expected)))))

(defn get-ex-code [ex]
  (-> ex .getCause .getResultCode))

(deftest add-bins-test
  (testing "it should add bins to an existing record without modifying old data"
    (c/set-single client "person" nil {"fname" "John" "lname" "Baker"} 30)
    (c/add-bins client "person" nil {"prefix" "Mr."} 60)

    (let [expected {:payload {"fname" "John" "lname" "Baker" "prefix" "Mr."} :ttl 60 :gen 1}
          actual   (c/get-single client "person" nil)]
      (is (= @actual expected))))

  (testing "it should throw an exception if key doesn't exist"
    (try
      @(c/add-bins client "does-not-exist" nil {"prefix" "Mr."} 60)
      (throw (AssertionError. "Expected AerospikeException to be thrown in `add-bins-test`"))
      (catch ExecutionException ex
        (is (= (get-ex-code ex) ResultCode/KEY_NOT_FOUND_ERROR))))))

(deftest create-test
  (testing "it should create a new record"
    (c/create client "foo" "set1" "bar" 60)
    (c/create client "baz" nil {"value" 1} 30)

    (let [expected {:payload "bar" :ttl 60 :gen 1}
          actual   (c/get-single client "foo" "set1")]
      (is (= @actual expected)))

    (let [expected {:payload {"value" 1} :ttl 30 :gen 1}
          actual   (c/get-single client "baz" nil)]
      (is (= @actual expected))))

  (testing "it should throw an exception when a key already exists"
    (doseq [args [["key1" "set1" "val1" 60] ["key2" nil "val2" 60]]]
      (apply c/create client args)
      (try
        @(apply c/create client args)
        (throw (AssertionError.
                 (str "Expected AerospikeException to be thrown in `create-test`: " args)))
        (catch ExecutionException ex
          (is (= (get-ex-code ex) ResultCode/KEY_EXISTS_ERROR)))))))

(deftest touch-test
  (testing "it should update the record's ttl if it exists"
    (c/create client "foo" "set1" "bar" 60)
    (c/touch client "foo" "set1" 10)

    (let [expected {:payload "bar" :ttl 10 :gen 1}
          actual   (c/get-single client "foo" "set1")]
      (is (= @actual expected))))

  (testing "it should throw an exception if a key doesn't exist"
    (try
      @(c/touch client "does-not-exist" nil 120)
      (throw (AssertionError. "Expected AerospikeException to be thrown in `touch-test`"))
      (catch ExecutionException ex
        (is (= (get-ex-code ex) ResultCode/KEY_NOT_FOUND_ERROR))))))

(deftest delete-test
  (testing "it should delete a record if it exists"
    (c/create client "foo" "set1" "bar" 60)
    (is (= @(c/delete client "foo" "set1") true))
    (is (= @(c/get-single client "foo" "set1") nil))

    (c/create client "foo" nil "bar" 30)
    (is (= @(c/delete client "foo" nil) true))
    (is (= @(c/get-single client "foo" nil) nil)))

  (testing "it should return false if a key doesn't exist"
    (c/create client "foo" nil "bar" 60)
    (c/create client "foo" "set1" "bar" 60)
    (let [default-set (mock/get-state client)
          set1        (mock/get-state client "set1")]
      (is (= @(c/delete client "does-not-exist" nil) false))
      (is (= @(c/delete client "does-not-exist" "foo-bar") false))

      (is (= default-set (mock/get-state client)))
      (is (= set1 (mock/get-state client "set1"))))))

(deftest delete-bins-test
  (testing "it should delete the given bins in an existing record without modifying others"
    (c/create client "person" nil {"fname" "John" "lname" "Baker" "prefix" "Mr." "age" 74} 30)
    (c/delete-bins client "person" nil ["prefix" "age"] 60)

    (let [expected {:payload {"fname" "John" "lname" "Baker"} :ttl 60 :gen 1}
          actual   (c/get-single client "person" nil)]
      (is (= @actual expected)))

    (c/delete-bins client "person" nil ["prefix" "age"] 10)
    (let [expected {:payload {"fname" "John" "lname" "Baker"} :ttl 10 :gen 1}
          actual   (c/get-single client "person" nil)]
      (is (= @actual expected))))

  (testing "it should throw an exception if key doesn't exist"
    (try
      @(c/delete-bins client "does-not-exist" nil ["fname"] 60)
      (throw (AssertionError. "Expected AerospikeException to be thrown in `delete-bins-test`"))
      (catch ExecutionException ex
        (is (= (get-ex-code ex) ResultCode/KEY_NOT_FOUND_ERROR))))))

(deftest expiry-unix-test
  (testing "it should proxy to aerospike-clj"
    (is (= (c/expiry-unix 100) (c/expiry-unix 100)))))

(deftest operate-test
  (testing "function is not implemented"
    (try
      (c/operate client "foo" nil 120 [])
      (throw (AssertionError. "Expected RuntimeException to be thrown in `operate-test`"))
      (catch RuntimeException _
        (is (= 1 1))))))

(deftest scan-set-test
  (let [test-data [["John Barker" {"fname" "John" "lname" "Baker" "prefix" "Mr." "age" 74}]
                   ["Fred Marshall" {"fname" "Fred" "lname" "Marshall" "prefix" "Mr." "age" 45}]
                   ["Gina Benson" {"fname" "Gina" "lname" "Benson" "prefix" "Mrs." "age" 39}]]]

    (doseq [v test-data]
      (c/create client (first v) "people" (second v) 30))

    (testing "it should scan the set and call the user supplied callback for each record"
      (let [res      (atom [])
            callback (fn [k v] (swap! res conj [k (get (:payload v) "age")]))]
        (is (true? @(c/scan-set client nil "people" {:callback callback})))
        (is (= [["John Barker" 74] ["Fred Marshall" 45] ["Gina Benson" 39]] @res))))

    (testing "it should abort the scan and return false when the callback returns :abort-scan"
      (let [callback (fn [_ v] (when (= (get (:payload v) "age") 45) :abort-scan))]
        (is (false? @(c/scan-set client nil "people" {:callback callback})))))

    (testing "it should include only the bins requested by the user"
      (let [res      (atom [])
            callback (fn [k v] (swap! res conj [k (:payload v)]))]
        (is (true? @(c/scan-set client nil "people" {:bins ["age"] :callback callback})))
        (is (= [["John Barker" {"age" 74}] ["Fred Marshall" {"age" 45}] ["Gina Benson" {"age" 39}]] @res))))

    (testing "it should scan the given namespace"
      (let [res      (atom [])
            callback (fn [_ v] (swap! res conj v))]
        (is (true? @(c/scan-set client nil "children" {:callback callback})))
        (is (empty @res))))))

(deftest healthy-test
  (is (= (c/healthy? client 0) true)))

(deftest get-cluster-stats-test
  (is (= [[]] (c/get-cluster-stats client))))

(deftest apply-transcoder-test
  (testing "it should apply the transcoder function to the payload before inserting it"
    (let [conf     {:transcoder #(clojure.core/update % "bar" inc)}
          expected {:payload {"bar" 21} :ttl 86400 :gen 1}]

      (c/put client "foo" "my-set" {"bar" 20} 86400 conf)
      (is (= @(c/get-single client "foo" "my-set") expected))))

  (testing "it should apply the transcoder function to the whole record before returning it"
    (let [conf     {:transcoder #(clojure.core/update-in % [:payload "bar"] inc)}
          expected {:payload {"bar" 22} :ttl 86400 :gen 1}
          actual   (c/get-single client "foo" "my-set" conf)]
      (is (= @actual expected))))

  (testing "it should apply the transcoder function on each record before returning them"
    (c/put-multiple client ["a" "b" "c"] [nil nil nil] ["d" "e" "f"] [10 20 30])

    (let [upper-case-value #(clojure.core/update % :payload clojure.string/upper-case)
          increment-ttl    #(clojure.core/update % :ttl inc)
          conf             {:transcoder (comp increment-ttl upper-case-value)}
          expected         [{:payload "D" :ttl 11 :gen 1}
                            {:payload "E" :ttl 21 :gen 1}
                            {:payload "F" :ttl 31 :gen 1}]
          actual           @(c/get-multiple client ["a" "b" "c"] [nil nil nil] conf)]
      (is (= actual expected)))))
