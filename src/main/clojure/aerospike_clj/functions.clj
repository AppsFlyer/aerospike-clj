(ns aerospike-clj.functions
  (:require [aerospike-clj.aerospike-record :as record])
  (:import (java.util.function Function)))

(def ^Function identity-function
  (reify Function
    (apply [_ x]
      x)))

(def ^Function record->map-function
  (reify Function
    (apply [_ record]
      (record/record->map record))))

(def ^Function mapv-batch-record->map-function
  (reify Function
    (apply [_ batch-records]
      (mapv record/batch-record->map batch-records))))

(def ^Function extract-payload-function
  (reify Function
    (apply [_ m]
      (:payload m))))

(def ^Function vec-function
  (reify Function
    (apply [_ batch-response]
      (vec batch-response))))
