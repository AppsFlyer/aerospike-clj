(ns aerospike-clj.aerospike-record
  (:require [aerospike-clj.utils :as utils])
  (:import [com.aerospike.client Record]))

(defrecord AerospikeRecord [payload ^Integer gen ^Integer ttl])

(defn record->map [^Record record]
  (and record
       (let [bins      (into {} (.bins record)) ;; converting from java.util.HashMap to a Clojure map
             bin-names (keys bins)
             payload   (if (utils/single-bin? bin-names)
                         ;; single bin record
                         (utils/desanitize-bin-value (get bins ""))
                         ;; multiple-bin record
                         (reduce-kv (fn [m k v]
                                      (assoc m k (utils/desanitize-bin-value v)))
                                    {}
                                    bins))]
         (->AerospikeRecord
           payload
           ^Integer (.generation ^Record record)
           ^Integer (.expiration ^Record record)))))
