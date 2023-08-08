(ns aerospike-clj.aerospike-record
  (:require [aerospike-clj.utils :as utils])
  (:import (com.aerospike.client BatchRecord Record)
           (java.util HashMap Map)
           (java.util.function BiConsumer)))

(defrecord AerospikeRecord [payload ^long gen ^long ttl])
(defrecord AerospikeBatchRecord [payload ^long gen ^long ttl index set ^long result-code])

(defn- single-bin?
  "Predicate function to determine whether data will be stored as a single bin or
  multiple bin record."
  [^Map bins]
  (and (= (.size bins) 1)
       (.containsKey bins "")))

(defn- Record->payload [^Record record]
  (let [bins (.bins record)]
    (when (some? bins)
      (if (single-bin? bins)
        ;; single bin record
        (utils/desanitize-bin-value (.get bins ""))
        ;; multiple-bin record
        (let [res (HashMap. (.size bins))]
          (.forEach bins
                    (reify BiConsumer
                      (accept [_ k v]
                        (.put res k (utils/desanitize-bin-value v)))))
          (into {} res))))))

(defn record->map [^Record record]
  (when (some? record)
    (->AerospikeRecord
      (Record->payload record)
      (.generation record)
      (.expiration record))))

(defn batch-record->map [^BatchRecord batch-record]
  (let [k      (.key batch-record)
        record (.record batch-record)]
    (->AerospikeBatchRecord
      (Record->payload record)
      (.generation record)
      (.expiration record)
      (.toString (.userKey k))
      (.setName k)
      (.resultCode batch-record))))
