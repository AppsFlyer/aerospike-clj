(ns aerospike-clj.key-test
  (:require [aerospike-clj.key :refer [create-key]]
            [clojure.test :refer [deftest is]]
            [taoensso.timbre :refer [spy]]
            [clojure.string :as s])
  (:import [com.aerospike.client Key Value]
           [com.aerospike.client.util ThreadLocalData]))

(defn- compare-keys 
  ([_k1] true)
  ([k1 k2] (= (spy (seq (.digest ^Key k1)))
              (spy (seq (.digest ^Key k2)))))
  ([^Key k1 ^Key k2 & more]
   (if (= (seq (.digest ^Key k1))
          (seq (.digest ^Key k2)))
     (if (next more)
       (recur k2 (first more) (next more))
       (compare-keys k2 (first more)))
     false)))
      
(deftest create-with-digest
  (let [as-ns "ns"
        as-set "set"
        as-key "key" 
        k (create-key as-key as-ns as-set)
        k-digest (.digest k)]
    (is (.equals k 
                 (create-key k-digest as-ns as-set (Value/get "user-key"))))))

(deftest create-with-digest-and-byte-array
  (let [as-ns "ns"
        as-set "set"
        ba (byte-array 20)]
      (doseq [i (range 20)]
        (aset-byte ba i i))
      (let [k (create-key ba as-ns as-set nil)
            k-digest (.digest k)]
        (is (.equals k 
                     (create-key k-digest as-ns as-set nil))))))

(deftest create-with-numbers
  (let [as-ns "ns"
        as-set "set"]
    (is (.equals (create-key 1000 as-ns as-set)
                 (create-key (int 1000) as-ns as-set)))))

(deftest too-long-key
  (let [too-long-key (s/join "" (repeat (ThreadLocalData/DefaultBufferSize) "k"))
        too-long-ba (byte-array (ThreadLocalData/DefaultBufferSize))
        too-long-value (Value/get (byte-array (ThreadLocalData/DefaultBufferSize)))]
    (is (thrown-with-msg? Exception #"key is too long"
                          (create-key too-long-key "ns" nil)))
    (is (thrown-with-msg? Exception #"key is too long"
                          (create-key too-long-key "ns" "set")))
    (is (thrown-with-msg? Exception #"key is too long"
                          (create-key too-long-ba "ns" "set")))
    (is (thrown-with-msg? Exception #"key is too long"
                          (create-key too-long-value "ns" "set")))
    (is (thrown-with-msg? Exception #"digest has to exactly 20 bytes long"
                          (create-key (byte-array 19) "ns" "set" nil)))))

