(ns aerospike-clj.bins
  (:import (clojure.lang IPersistentMap)
           (com.aerospike.client Bin Value)
           (com.aerospike.client.policy RecordExistsAction)
           (java.util ArrayList)))

(set! *warn-on-reflection* true)

(def ^:private ^:const MAX_BIN_NAME_LENGTH 14)

(defn- check-bin-name-length [^String bin-name]
  (when (< MAX_BIN_NAME_LENGTH (.length bin-name))
    (throw (Exception. (format "%s is %s characters. Bin names have to be <= %d characters..." bin-name (.length bin-name) MAX_BIN_NAME_LENGTH)))))

(defn- create-bin ^Bin [^String bin-name bin-value]
  (check-bin-name-length bin-name)
  (if (nil? bin-value)
    ;; Aerospike bins can't hold a null value; writing nil instead removes the
    ;; bin, so on read the key is simply absent (which reads back as nil).
    (Bin/asNull bin-name)
    ;; Value/get throws "Unsupported type" for values the client can't store natively.
    (Bin. bin-name (Value/get ^Object bin-value))))

(defn set-bin-as-null ^Bin [^String bin-name]
  (check-bin-name-length bin-name)
  (Bin/asNull bin-name))

(defn- map->multiple-bins ^"[Lcom.aerospike.client.Bin;" [^IPersistentMap m omit-nil-bins?]
  (let [res (ArrayList. (.count m))]
    (doseq [[bin-name bin-value] m]
      (when-not (string? bin-name)
        (throw (Exception. (format "Aerospike only accepts string values as bin names. Please ensure all keys in the map are strings."))))
      (if (and omit-nil-bins? (nil? bin-value))
        (check-bin-name-length bin-name)
        (.add res (create-bin bin-name bin-value))))
    (.toArray res ^"[Lcom.aerospike.client.Bin;" (make-array Bin 0))))

(defn data->bins
  "Function to identify whether `data` will be stored as a single or multiple bin record.
  Only Clojure maps will default to multiple bins. Nested data structures are supported.

  A `nil` bin value removes the bin. With `omit-nil-bins?` (REPLACE writes, where the server rejects
  bin deletes) `nil` bins are left out of the write instead. With `require-value?` (REPLACE and
  CREATE_ONLY writes) a map whose bins are all `nil` throws. `nil` data always throws."
  (^"[Lcom.aerospike.client.Bin;" [data]
   (data->bins data false false))
  (^"[Lcom.aerospike.client.Bin;" [data omit-nil-bins? require-value?]
   (cond
     (nil? data)
     (throw (IllegalArgumentException. "A record can't be written as nil. Use delete to remove it"))

     (map? data)
     (let [bins (map->multiple-bins data omit-nil-bins?)]
       (when (and require-value? (seq data) (every? nil? (vals data)))
         (throw (IllegalArgumentException. "A REPLACE or CREATE_ONLY write needs at least one non-nil bin value")))
       bins)

     :else
     (doto ^"[Lcom.aerospike.client.Bin;" (make-array Bin 1)
       (aset 0 (create-bin "" data))))))

(defn data->write-bins
  "`data` as bins for a write whose `RecordExistsAction` is `action`, with the `nil` rules of `data->bins`."
  ^"[Lcom.aerospike.client.Bin;" [data ^RecordExistsAction action]
  (let [replace? (or (= RecordExistsAction/REPLACE action)
                     (= RecordExistsAction/REPLACE_ONLY action))]
    (data->bins data replace? (or replace? (= RecordExistsAction/CREATE_ONLY action)))))
