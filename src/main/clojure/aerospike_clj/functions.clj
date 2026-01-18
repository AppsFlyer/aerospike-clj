(ns aerospike-clj.functions
  (:require [aerospike-clj.aerospike-record :as record]
            [aerospike-clj.collections :as collections])
  (:import (java.util.function Function)))

(def ^Function record->map-function
  (reify Function
    (apply [_ record]
      (record/record->map record))))

(def ^Function batch-record->defrecord-function
  (reify Function
    (apply [_ batch-records]
      (collections/->list record/batch-record->map batch-records))))

(def ^Function extract-payload-function
  (reify Function
    (apply [_ m]
      (:payload m))))

(def ^Function vec-function
  (reify Function
    (apply [_ batch-response]
      (vec batch-response))))
