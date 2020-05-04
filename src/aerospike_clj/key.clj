(ns aerospike-clj.key
  (:import [com.aerospike.client Key Value]
           [com.aerospike.client.util ThreadLocalData]))

(defprotocol AerospikeIndex
  (create-key-method ^Key [this ^String as-namesapce ^String set-name]))

(extend-protocol AerospikeIndex
  (Class/forName "[B") ;; byte-array 
  (create-key-method ^Key [this ^String as-namesapce ^String set-name]
    (let [set-name-length (if set-name
                            (.length set-name)
                            0)]
      (when (<= (ThreadLocalData/DefaultBufferSize) (+ (alength #^bytes this) set-name-length))
        (throw (Exception. (format "key is too long: %s..." this)))))
    (Key. ^String as-namesapce ^String set-name #^bytes this))
  String 
  (create-key-method ^Key [this ^String as-namesapce ^String set-name]
    (let [set-name-length (if set-name
                            (.length set-name)
                            0)]
      (when (<= (ThreadLocalData/DefaultBufferSize) (+ (.length this) set-name-length))
        (throw (Exception. (format "key is too long: %s..." (subs this 0 40))))))
    (Key. as-namesapce set-name this))
  Integer 
  (create-key-method ^Key [this ^String as-namesapce ^String set-name]
    (Key. as-namesapce set-name (.longValue this)))
  Long 
  (create-key-method ^Key [this ^String as-namesapce ^String set-name]
    (Key. as-namesapce set-name (.longValue this)))
  Value 
  (create-key-method ^Key [this ^String as-namesapce ^String set-name]
    (let [set-name-length (if set-name
                            (.length set-name)
                            0)]
      (when (<= (ThreadLocalData/DefaultBufferSize) (+ (.estimateSize this) set-name-length))
        (throw (Exception. (format "key is too long: %s..." (subs (.toString this) 0 40))))))
    (Key. as-namesapce set-name this)))

(defn create-key
  "Create an aerospike key. It is recommended to create simple keys (without 
  precomputed digest) with `client/create-key`"
  (^Key [k ^String as-namesapce ^String set-name]
   (create-key-method k as-namesapce set-name))
  (^Key [#^bytes digest ^String as-namesapce ^String set-name ^Value user-key]
    (when (not= 20 (alength digest))
      (throw (Exception. "digest has to exactly 20 bytes long")))
   (Key. as-namesapce digest set-name user-key)))
