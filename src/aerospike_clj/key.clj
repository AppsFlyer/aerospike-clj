(ns aerospike-clj.key
  (:import [com.aerospike.client Key Value]
           [com.aerospike.client.util ThreadLocalData]))

(defprotocol AerospikeIndex
  (create-key-method ^Key [this ^String as-namesapce ^String set-name]))

(extend-protocol AerospikeIndex
  (Class/forName "[B") 
  (create-key-method ^Key [this ^String as-namesapce ^String set-name]
    (when (< (ThreadLocalData/DefaultBufferSize) (+ 1 (alength #^bytes this) (.length set-name)))
      (throw (Exception. (format "key is too long: %s..." this))))
    (Key. ^String as-namesapce ^String set-name #^bytes this))
  String 
  (create-key-method ^Key [this ^String as-namesapce ^String set-name]
    (when (< (ThreadLocalData/DefaultBufferSize) (+ 1 (.length this) (.length set-name)))
      (throw (Exception. (format "key is too long: %s..." (subs this 0 40)))))
    (Key. as-namesapce set-name this))
  Integer 
  (create-key-method ^Key [this ^String as-namesapce ^String set-name]
    (Key. as-namesapce set-name (.longValue this)))
  Long 
  (create-key-method ^Key [this ^String as-namesapce ^String set-name]
    (Key. as-namesapce set-name (.longValue this)))
  Value 
  (create-key-method ^Key [this ^String as-namesapce ^String set-name]
    (when (< (ThreadLocalData/DefaultBufferSize) (+ 1 (.estimateSize this) (.length set-name)))
      (throw (Exception. (format "key is too long: %s..." (subs (.toString this) 0 40)))))
    (Key. as-namesapce set-name this)))

(defn create-key
  "Create an aerospike key. It is recommended to create simple keys (without 
  precomputed digest) with `client/create-key`"
  (^Key [k ^String as-namesapce ^String set-name]
   (create-key-method k as-namesapce set-name))
  (^Key [#^bytes digest ^String as-namesapce ^String set-name ^Value user-key]
   (Key. as-namesapce digest set-name user-key)))
