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

(defn- map->multiple-bins ^"[Lcom.aerospike.client.Bin;@4fee060b" [^IPersistentMap m]
  (let [bin-names (keys m)]
    (if (utils/string-keys? bin-names)
      (utils/v->array Bin #(create-bin (key %) (utils/sanitize-bin-value (val %))))
      (throw (Exception. (format "Aerospike only accepts string values as bin names. Please ensure all keys in the map are strings."))))))

(defn data->bins
  "Function to identify whether `data` will be stored as a single or multiple bin record.
  Only Clojure maps will default to multiple bins. Nested data structures are supported."
  [data]
  (if (map? data)
    (map->multiple-bins data)
    (doto (make-array Bin 1)
      (aset 0 (Bin. "" (utils/sanitize-bin-value data))))))
