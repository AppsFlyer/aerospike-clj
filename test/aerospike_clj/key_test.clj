(ns aerospike-clj.key-test
  (:require [aerospike-clj.key :refer [create-key]]
            [clojure.test :refer [deftest is]]
            [clojure.string :as s])
  (:import [java.util UUID]
           [com.aerospike.client Value]
           [com.aerospike.client.util ThreadLocalData]
           [com.appsflyer.AerospikeClj ByteUtils]))

(deftest create-with-digest
  (let [as-ns    "ns"
        as-set   "set"
        as-key   "key"
        k        (create-key as-key as-ns as-set)
        k-digest (.digest k)]
    (is (.equals k
                 (create-key k-digest as-ns as-set (Value/get "user-key"))))))

(deftest create-with-digest-and-byte-array
  (let [as-ns  "ns"
        as-set "set"
        ba     (byte-array 20)]
    (doseq [i (range 20)]
      (aset-byte ba i i))
    (let [k        (create-key ba as-ns as-set nil)
          k-digest (.digest k)]
      (is (.equals k
                   (create-key k-digest as-ns as-set nil))))))

(deftest create-with-numbers
  (let [as-ns  "ns"
        as-set "set"]
    (is (.equals (create-key 1000 as-ns as-set)
                 (create-key (int 1000) as-ns as-set)))))

(deftest byte-utils-round-trip
  (let [uuid (UUID/randomUUID)]
    (is (= uuid
           (-> uuid
               ByteUtils/bytesFromUUID
               ByteUtils/uuidFromBytes)))))

(deftest create-with-uuid
  (let [as-ns    "ns"
        as-set   "set"
        uuid     (UUID/randomUUID)
        k        (create-key uuid as-ns as-set)
        k-digest (.digest k)]
    (is (.equals k
                 (create-key k-digest as-ns as-set nil)))))

(deftest too-long-key
  (let [too-long-key   (s/join "" (repeat (ThreadLocalData/DefaultBufferSize) "k"))
        too-long-ba    (byte-array (ThreadLocalData/DefaultBufferSize))
        too-long-value (Value/get (byte-array (ThreadLocalData/DefaultBufferSize)))]
    (is (thrown-with-msg? Exception #"key is too long"
                          (create-key too-long-key "ns" nil)))
    (is (thrown-with-msg? Exception #"key is too long"
                          (create-key too-long-key "ns" "set")))
    (is (thrown-with-msg? Exception #"key is too long"
                          (create-key too-long-ba "ns" "set")))
    (is (thrown-with-msg? Exception #"key is too long"
                          (create-key too-long-value "ns" "set")))
    (is (thrown-with-msg? Exception #"digest has to be exactly 20 bytes long"
                          (create-key (byte-array 19) "ns" "set" nil)))))

