(ns aerospike-clj.futures
  (:import (java.util Collection)
           (java.util.concurrent CompletableFuture)))

(set! *warn-on-reflection* true)

(defn get-now-or-nil [^CompletableFuture cf]
  (.getNow cf nil))

(def ^"[Ljava.util.concurrent.CompletableFuture;" zeros-length-futures-array
  "Empty array of CompletableFuture, it's used to create a Java array from a [[java.util.Collection]] of CompletableFutures."
  (make-array CompletableFuture 0))

(defn all-of
  "Wait for all futures in the collection to complete, and return a future that completes when all of them are done.
   The returned future will complete with a [[java.lang.Void]] value."
  ^CompletableFuture [^Collection coll]
  (CompletableFuture/allOf (.toArray coll zeros-length-futures-array)))
