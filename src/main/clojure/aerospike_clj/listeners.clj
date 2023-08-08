(ns aerospike-clj.listeners
  (:require [aerospike-clj.aerospike-record :as record])
  (:import (com.aerospike.client AerospikeException AerospikeException$QueryTerminated Key Record)
           (com.aerospike.client.listener BatchListListener BatchOperateListListener DeleteListener
                                          ExistsArrayListener ExistsListener InfoListener RecordListener RecordSequenceListener WriteListener)
           (java.util List Map)
           (java.util.concurrent CompletableFuture)))

(deftype AsyncExistsListener [^CompletableFuture op-future]
  ExistsListener
  (^void onFailure [_this ^AerospikeException ex]
    (.completeExceptionally op-future ex))
  (^void onSuccess [_this ^Key _k ^boolean exists]
    (.complete op-future exists)))

(deftype AsyncDeleteListener [^CompletableFuture op-future]
  DeleteListener
  (^void onSuccess [_this ^Key _k ^boolean existed]
    (.complete op-future existed))
  (^void onFailure [_ ^AerospikeException ex]
    (.completeExceptionally op-future ex)))

(deftype AsyncWriteListener [^CompletableFuture op-future]
  WriteListener
  (^void onSuccess [_this ^Key _]
    (.complete op-future true))
  (^void onFailure [_this ^AerospikeException ex]
    (.completeExceptionally op-future ex)))

(deftype AsyncInfoListener [^CompletableFuture op-future]
  InfoListener
  (^void onSuccess [_this ^Map result-map]
    (.complete op-future (into {} result-map)))
  (^void onFailure [_this ^AerospikeException ex]
    (.completeExceptionally op-future ex)))

(deftype AsyncRecordListener [^CompletableFuture op-future]
  RecordListener
  (^void onFailure [_this ^AerospikeException ex]
    (.completeExceptionally op-future ex))
  (^void onSuccess [_this ^Key _k ^Record record]
    (.complete op-future record)))

(deftype AsyncRecordSequenceListener [^CompletableFuture op-future callback]
  RecordSequenceListener
  (^void onRecord [_this ^Key k ^Record record]
    (when (= :abort-scan (callback (.userKey k) (record/record->map record)))
      (throw (AerospikeException$QueryTerminated.))))
  (^void onSuccess [_this]
    (.complete op-future true))
  (^void onFailure [_this ^AerospikeException exception]
    (if (instance? AerospikeException$QueryTerminated exception)
      (.complete op-future false)
      (.completeExceptionally op-future exception))))

(deftype AsyncBatchListListener [^CompletableFuture op-future]
  BatchListListener
  (^void onFailure [_this ^AerospikeException ex]
    (.completeExceptionally op-future ex))
  (^void onSuccess [_this ^List records]
    (.complete op-future records)))

(deftype AsyncExistsArrayListener [^CompletableFuture op-future]
  ExistsArrayListener
  (^void onFailure [_this ^AerospikeException ex]
    (.completeExceptionally op-future ex))
  (^void onSuccess [_this ^"[Lcom.aerospike.client.Key;" _keys ^"[Z" exists]
    (.complete op-future exists)))

(deftype AsyncBatchOperateListListener [^CompletableFuture op-future]
  BatchOperateListListener
  (^void onSuccess [_this ^List records ^boolean _status]
    (.complete op-future records))
  (^void onFailure [_this ^AerospikeException ex]
    (.completeExceptionally op-future ex)))
