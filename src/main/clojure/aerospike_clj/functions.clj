(ns aerospike-clj.functions
  (:require [aerospike-clj.aerospike-record :as record])
  (:import (java.util.function Function)))

(def ^Function identity
  (reify Function
    (apply [_ x]
      x)))

(def ^Function record->map
  (reify Function
    (apply [_ record]
      (record/record->map record))))

(def ^Function mapv-batch-record->map
  (reify Function
    (apply [_ batch-records]
      (mapv record/batch-record->map batch-records))))

(def ^Function extract-payload
  (reify Function
    (apply [_ m]
      (:payload m))))

(def ^Function ->vec
  (reify Function
    (apply [_ batch-response]
      (vec batch-response))))
