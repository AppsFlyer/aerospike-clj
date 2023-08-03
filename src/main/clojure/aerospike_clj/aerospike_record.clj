(ns aerospike-clj.aerospike-record
  (:require [aerospike-clj.utils :as utils])
  (:import (com.aerospike.client Record)
           (java.util Map)))

(defrecord AerospikeRecord [payload ^Integer gen ^Integer ttl])

(defn- single-bin?
  "Predicate function to determine whether data will be stored as a single bin or
  multiple bin record."
  [^Map bins]
  (and (= (.size bins) 1)
       (.containsKey bins "")))

(defn record->map [^Record record]
  (and record
       (let [bins    ^Map (.bins record)
             payload (when (some? bins)
                       (if (single-bin? bins)
                         ;; single bin record
                         (utils/desanitize-bin-value (.get bins ""))
                         ;; multiple-bin record
                         (reduce-kv (fn [m k v]
                                      (assoc m k (utils/desanitize-bin-value v)))
                                    {}
                                    bins)))]
         (->AerospikeRecord
           payload
           ^Integer (.generation ^Record record)
           ^Integer (.expiration ^Record record)))))
