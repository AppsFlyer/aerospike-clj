(ns aerospike-clj.bins
  (:require [aerospike-clj.utils :as utils])
  (:import (clojure.lang IPersistentMap)
           (com.aerospike.client Bin)))

(def ^:private ^:const MAX_BIN_NAME_LENGTH 14)

(defn- create-bin ^Bin [^String bin-name bin-value]
  (when (< MAX_BIN_NAME_LENGTH (.length bin-name))
    (throw (Exception. (format "%s is %s characters. Bin names have to be <= %d characters..." bin-name (.length bin-name) MAX_BIN_NAME_LENGTH))))
  (Bin. bin-name bin-value))

(defn set-bin-as-null ^Bin [^String bin-name]
  (when (< MAX_BIN_NAME_LENGTH (.length bin-name))
    (throw (Exception. (format "%s is %s characters. Bin names have to be <= %d characters..." bin-name (.length bin-name) MAX_BIN_NAME_LENGTH))))
  (Bin/asNull bin-name))

(defn- map->multiple-bins ^"[Lcom.aerospike.client.Bin;" [^IPersistentMap m]
  (let [size     (.count m)
        iterator (.iterator m)
        res      (make-array Bin size)]
    (loop [i 0]
      (when (and (< i size)
                 (.hasNext iterator))
        (let [entry     (.next iterator)
              key-entry (key entry)]
          (when-not (string? key-entry)
            (throw (Exception. (format "Aerospike only accepts string values as bin names. Please ensure all keys in the map are strings."))))
          (aset res i (create-bin key-entry (utils/sanitize-bin-value (val entry))))
          (recur (inc i)))))
    res))

(defn data->bins
  "Function to identify whether `data` will be stored as a single or multiple bin record.
  Only Clojure maps will default to multiple bins. Nested data structures are supported."
  ^"[Lcom.aerospike.client.Bin;" [data]
  (if (map? data)
    (map->multiple-bins data)
    (doto ^"[Lcom.aerospike.client.Bin;" (make-array Bin 1)
      (aset 0 (Bin. "" (utils/sanitize-bin-value data))))))
