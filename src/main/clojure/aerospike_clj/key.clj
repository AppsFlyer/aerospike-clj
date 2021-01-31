(ns aerospike-clj.key
  (:require [aerospike-clj.protocols :refer [AerospikeIndex create-key-method]])
  (:import [java.util UUID]
           [com.aerospike.client Key Value]
           [com.aerospike.client.util ThreadLocalData]
           [com.appsflyer.AerospikeClj ByteUtils]))

; A Java array of primitive `byte` has no Clojure symbol that can resolve to the
; class, so we use `extend` to preserve type hints and prevent tools like Cursive
; from having issues with analyzing the `extend-protocol` form.
(extend (Class/forName "[B")
  AerospikeIndex
  {:create-key-method (fn [^bytes this ^String as-namesapce ^String set-name]
                        (let [set-name-length (if set-name
                                                (.length set-name)
                                                0)]
                          (when (<= (ThreadLocalData/DefaultBufferSize) (+ (alength this) set-name-length))
                            (throw (Exception. (format "key is too long: %s..." this)))))
                        (Key. as-namesapce set-name this))})

(extend-protocol AerospikeIndex
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

  UUID
  (create-key-method ^Key [^UUID this as-namesapce set-name]
    (create-key-method (ByteUtils/bytesFromUUID this) as-namesapce set-name))

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
  precomputed digest) with `client/create-key`."
  (^Key [k ^String as-namesapce ^String set-name]
   (create-key-method k as-namesapce set-name))

  (^Key [#^bytes digest ^String as-namesapce ^String set-name ^Value user-key]
    (when (not= 20 (alength digest))
      (throw (Exception. "digest has to be exactly 20 bytes long")))
   (Key. as-namesapce digest set-name user-key)))
