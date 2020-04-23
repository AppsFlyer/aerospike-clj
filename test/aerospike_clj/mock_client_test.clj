(ns aerospike-clj.mock-client-test
  (:refer-clojure :exclude [update])
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [aerospike-clj.mock-client :as mock]
            [aerospike-clj.client :as client]
            [clojure.string])
  (:import [com.aerospike.client ResultCode AerospikeException]))

(def ^:dynamic ^mock/AerospikeClient client nil)

(defn- rebind-client-to-mock [test-fn]
  (binding [client (mock/create-instance)]
    (test-fn)))

(use-fixtures :each rebind-client-to-mock)

(deftest get-single-test
  (mock/put client "foo" "my-set" {"bar" 20} 86400)

  (testing "it should return a deferrable of a single record if it exists"
    (let [expected {:payload {"bar" 20} :ttl 86400 :gen 1}
          actual (mock/get-single client "foo" "my-set")]
      (is (= @actual expected))))

  (testing "it should return a deferrable of nil if it doesn't exist"
    (let [actual (mock/get-single client "foo" nil)]
      (is (= @actual nil)))
    (let [actual (mock/get-single client "does-not-exist" "my-set")]
      (is (= @actual nil)))))

(deftest get-single-no-meta-test
  (mock/put client "foo" nil "bar" 30)

  (testing "it should return a deferrable of a single record without metadata if it exists"
    (let [expected "bar"
          actual (mock/get-single-no-meta client "foo" nil)]
      (is (= @actual expected))))

  (testing "it should return a deferrable of nil if it doesn't exist"
    (let [actual (mock/get-single-no-meta client "does-not-exist" nil)]
      (is (= @actual nil)))))

(deftest get-multiple-test
  (testing "it should return a deferrable of vector of matched records"
    (mock/put-multiple client ["foo" "bar"] [nil "some-set"] ["is best" "is better"] [86400 43200])

    (let [expected [{:payload "is best" :ttl 86400 :gen 1}]
          actual (mock/get-multiple client ["foo"] [nil])]
      (is (= @actual expected)))

    (let [expected [{:payload "is better" :ttl 43200 :gen 1}]
          actual (mock/get-multiple client ["bar"] ["some-set"])]
      (is (= @actual expected)))

    (let [expected [{:payload "is best" :ttl 86400 :gen 1}
                    {:payload "is better" :ttl 43200 :gen 1}]
          actual (mock/get-multiple client ["foo" "bar"] [nil "some-set"])]
      (is (= @actual expected)))

    (let [expected [nil nil]
          actual (mock/get-multiple client ["does-not-exist" "also-not-here"] [nil "some-set"])]
      (is (= @actual expected)))))

(deftest exists?-test
  (testing "it should return a deferrable of boolean indicating whether a record exists"
    (mock/put client "foo" nil "bar" 30)
    (is (= @(mock/exists? client "foo" nil) true))
    (is (= @(mock/exists? client "bar" nil) false))
    (is (= @(mock/exists? client "FOO" nil) false))
    (is (= @(mock/exists? client "" nil) false))
    (is (= @(mock/exists? client nil nil) false))))

(deftest put-test
  (testing "it should create a new record if it doesn't exist"
    (mock/put client "foo" nil "bar" 30)
    (let [expected {:payload "bar" :ttl 30 :gen 1}
          actual (mock/get-single client "foo" nil)]
      (is (= @actual expected))))

  (testing "it should replace a record if it exists"
    (mock/put client "foo" nil "bar" 30)
    (let [expected {:payload "bar" :ttl 30 :gen 1}
          actual (mock/get-single client "foo" nil)]
      (is (= @actual expected)))

    (mock/put client "foo" nil "baz" 40)
    (let [expected {:payload "baz" :ttl 40 :gen 1}
          actual (mock/get-single client "foo" nil)]
      (is (= @actual expected)))))

(deftest set-single-test
  (testing "it should create a new record if it doesn't exist"
    (mock/set-single client "foo" nil "bar" 30)
    (let [expected {:payload "bar" :ttl 30 :gen 1}
          actual (mock/get-single client "foo" nil)]
      (is (= @actual expected))))

  (testing "it should update a record if it exists"
    (mock/set-single client "foo" nil "baz" 30)
    (let [expected {:payload "baz" :ttl 30 :gen 2}
          actual (mock/get-single client "foo" nil)]
      (is (= @actual expected)))

    (mock/set-single client "foo" nil "baz" 40)
    (let [expected {:payload "baz" :ttl 40 :gen 3}
          actual (mock/get-single client "foo" nil)]
      (is (= @actual expected)))))

(deftest put-multiple-test
  (testing "it should create a new record for each one that doesn't exist and replace each one
  that does"
    (let [put-result (mock/put-multiple client ["foo" "bar"] [nil nil] [1 2] [60 60])
          expected [{:payload 1 :ttl 60 :gen 1}
                    {:payload 2 :ttl 60 :gen 1}]
          actual (mock/get-multiple client ["foo" "bar"] [nil nil])]
      (is (= @put-result [true true]))
      (is (= @actual expected)))


    (let [put-result (mock/put-multiple client ["foo" "baz"] [nil nil] [3 4] [120 60])
          expected [{:payload 3 :ttl 120 :gen 1}
                    {:payload 2 :ttl 60 :gen 1}
                    {:payload 4 :ttl 60 :gen 1}]
          actual (mock/get-multiple client ["foo" "bar" "baz"] [nil nil nil])]
      (is (= @put-result [true true]))
      (is (= @actual expected)))))

(deftest add-bins-test
  (testing "it should add bins to an existing record without modifying old data"
    (mock/set-single client "person" nil {"fname" "John" "lname" "Baker"} 30)
    (mock/add-bins client "person" nil {"prefix" "Mr."} 60)

    (let [expected {:payload {"fname" "John" "lname" "Baker" "prefix" "Mr."} :ttl 60 :gen 1}
          actual (mock/get-single client "person" nil)]
      (is (= @actual expected))))

  (testing "it should throw an exception if key doesn't exist"
    (try
      @(mock/add-bins client "does-not-exist" nil {"prefix" "Mr."} 60)
      (throw (AssertionError. "Expected AerospikeException to be thrown in `add-bins-test`"))
      (catch AerospikeException ex
        (is (= (.getResultCode ex) ResultCode/KEY_NOT_FOUND_ERROR))))))

(deftest create-test
  (testing "it should create a new record"
    (mock/create client "foo" "set1" "bar" 60)
    (mock/create client "baz" nil {"value" 1} 30)

    (let [expected {:payload "bar" :ttl 60 :gen 1}
          actual (mock/get-single client "foo" "set1")]
      (is (= @actual expected)))

    (let [expected {:payload {"value" 1} :ttl 30 :gen 1}
          actual (mock/get-single client "baz" nil)]
      (is (= @actual expected))))

  (testing "it should throw an exception when a key already exists"
    (doseq [args [["key1" "set1" "val1" 60] ["key2" nil "val2" 60]]]
      (apply mock/create client args)
      (try
        @(apply mock/create client args)
        (throw (AssertionError.
                 (str "Expected AerospikeException to be thrown in `create-test`: " args)))
        (catch AerospikeException ex
          (is (= (.getResultCode ex) ResultCode/KEY_EXISTS_ERROR)))))))

(deftest touch-test
  (testing "it should update the record's ttl if it exists"
    (mock/create client "foo" "set1" "bar" 60)
    (mock/touch client "foo" "set1" 10)

    (let [expected {:payload "bar" :ttl 10 :gen 1}
          actual (mock/get-single client "foo" "set1")]
      (is (= @actual expected))))

  (testing "it should throw an exception if a key doesn't exist"
    (try
      @(mock/touch client "does-not-exist" nil 120)
      (throw (AssertionError. "Expected AerospikeException to be thrown in `touch-test`"))
      (catch AerospikeException ex
        (is (= (.getResultCode ex) ResultCode/KEY_NOT_FOUND_ERROR))))))

(deftest delete-test
  (testing "it should delete a record if it exists"
    (mock/create client "foo" "set1" "bar" 60)
    (is (= @(mock/delete client "foo" "set1") true))
    (is (= @(mock/get-single client "foo" "set1") nil))

    (mock/create client "foo" nil "bar" 30)
    (is (= @(mock/delete client "foo" nil) true))
    (is (= @(mock/get-single client "foo" nil) nil)))

  (testing "it should return false if a key doesn't exist"
    (mock/create client "foo" nil "bar" 60)
    (mock/create client "foo" "set1" "bar" 60)
    (let [default-set (mock/get-state client)
          set1 (mock/get-state client "set1")]
      (is (= @(mock/delete client "does-not-exist" nil) false))
      (is (= @(mock/delete client "does-not-exist" "foo-bar") false))

      (is (= default-set (mock/get-state client)))
      (is (= set1 (mock/get-state client "set1"))))))

(deftest delete-bins-test
  (testing "it should delete the given bins in an existing record without modifying others"
    (mock/create client "person" nil {"fname" "John" "lname" "Baker" "prefix" "Mr." "age" 74} 30)
    (mock/delete-bins client "person" nil ["prefix" "age"] 60)

    (let [expected {:payload {"fname" "John" "lname" "Baker"} :ttl 60 :gen 1}
          actual (mock/get-single client "person" nil)]
      (is (= @actual expected)))

    (mock/delete-bins client "person" nil ["prefix" "age"] 10)
    (let [expected {:payload {"fname" "John" "lname" "Baker"} :ttl 10 :gen 1}
          actual (mock/get-single client "person" nil)]
      (is (= @actual expected))))

  (testing "it should throw an exception if key doesn't exist"
    (try
      @(mock/delete-bins client "does-not-exist" nil ["fname"] 60)
      (throw (AssertionError. "Expected AerospikeException to be thrown in `delete-bins-test`"))
      (catch AerospikeException ex
        (is (= (.getResultCode ex) ResultCode/KEY_NOT_FOUND_ERROR))))))

(deftest expiry-unix-test
  (testing "it should proxy to aerospike-clj"
    (is (= (mock/expiry-unix 100) (client/expiry-unix 100)))))

(deftest operate-test
  (testing "function is not implemented"
    (try
      (mock/operate client "foo" nil 120 [])
      (throw (AssertionError. "Expected RuntimeException to be thrown in `operate-test`"))
      (catch RuntimeException _
        (is (= 1 1))))))

(deftest scan-set-test
  (let [test-data [["John Barker" {"fname" "John" "lname" "Baker" "prefix" "Mr." "age" 74}]
                   ["Fred Marshall" {"fname" "Fred" "lname" "Marshall" "prefix" "Mr." "age" 45}]
                   ["Gina Benson" {"fname" "Gina" "lname" "Benson" "prefix" "Mrs." "age" 39}]]]

    (doseq [v test-data]
      (mock/create client (first v) "people" (second v) 30))

    (testing "it should scan the set and call the user supplied callback for each record"
      (let [res (atom [])
            callback (fn [k v] (swap! res conj [k (get (:payload v) "age")]))]
        (is (true? @(mock/scan-set client nil "people" {:callback callback})))
        (is (= [["John Barker" 74] ["Fred Marshall" 45] ["Gina Benson" 39]] @res))))

    (testing "it should abort the scan and return false when the callback returns :abort-scan"
      (let [callback (fn [_ v] (when (= (get (:payload v) "age") 45) :abort-scan))]
        (is (false? @(mock/scan-set client nil "people" {:callback callback})))))

    (testing "it should include only the bins requested by the user"
      (let [res (atom [])
            callback (fn [k v] (swap! res conj [k (:payload v)]))]
        (is (true? @(mock/scan-set client nil "people" {:bins ["age"] :callback callback})))
        (is (= [["John Barker" {"age" 74}] ["Fred Marshall" {"age" 45}] ["Gina Benson" {"age" 39}]] @res))))

    (testing "it should scan the given namespace"
      (let [res (atom [])
            callback (fn [_ v] (swap! res conj v))]
        (is (true? @(mock/scan-set client nil "children" {:callback callback})))
        (is (empty @res))))))

(deftest healthy-test
  (is (= (mock/healthy? client 0) true)))

(deftest get-cluster-stats-test
  (is (= [[]] (mock/get-cluster-stats client))))

(deftest apply-transcoder-test
  (testing "it should apply the transcoder function to the payload before inserting it"
    (let [conf {:transcoder #(clojure.core/update % "bar" inc)}
          expected {:payload {"bar" 21} :ttl 86400 :gen 1}]

      (mock/put client "foo" "my-set" {"bar" 20} 86400 conf)
      (is (= @(mock/get-single client "foo" "my-set") expected))))

  (testing "it should apply the transcoder function to the whole record before returning it"
    (let [conf {:transcoder #(clojure.core/update-in % [:payload "bar"] inc)}
          expected {:payload {"bar" 22} :ttl 86400 :gen 1}
          actual (mock/get-single client "foo" "my-set" conf)]
      (is (= @actual expected))))

  (testing "it should apply the transcoder function on each record before returning them"
    (mock/put-multiple client ["a" "b" "c"] [nil nil nil] ["d" "e" "f"] [10 20 30])

    (let [upper-case-value #(clojure.core/update % :payload clojure.string/upper-case)
          increment-ttl #(clojure.core/update % :ttl inc)
          conf {:transcoder (comp increment-ttl upper-case-value)}
          expected [{:payload "D" :ttl 11 :gen 1}
                    {:payload "E" :ttl 21 :gen 1}
                    {:payload "F" :ttl 31 :gen 1}]
          actual @(mock/get-multiple client ["a" "b" "c"] [nil nil nil] conf)]
      (is (= actual expected)))))