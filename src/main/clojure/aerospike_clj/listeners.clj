(ns aerospike-clj.listeners
  (:require [promesa.core :as p]
            [aerospike-clj.aerospike-record :as record])
  (:import [com.aerospike.client Key Record AerospikeException AerospikeException$QueryTerminated]
           [com.aerospike.client.listener RecordListener WriteListener DeleteListener ExistsListener BatchListListener RecordSequenceListener
                                          InfoListener ExistsArrayListener]
           [java.util List Map]))

(defn ^ExistsListener reify-exists-listener [op-future]
  (reify ExistsListener
    (^void onFailure [_this ^AerospikeException ex]
      (p/reject! op-future ex))
    (^void onSuccess [_this ^Key _k ^boolean exists]
      (p/resolve! op-future exists))))

(defn ^DeleteListener reify-delete-listener [op-future]
  (reify
    DeleteListener
    (^void onSuccess [_this ^Key _k ^boolean existed]
      (p/resolve! op-future existed))
    (^void onFailure [_ ^AerospikeException ex]
      (p/reject! op-future ex))))

(defn ^WriteListener reify-write-listener [op-future]
  (reify
    WriteListener
    (^void onSuccess [_this ^Key _]
      (p/resolve! op-future true))
    (^void onFailure [_this ^AerospikeException ex]
      (p/reject! op-future ex))))

(defn ^InfoListener reify-info-listener [op-future]
  (reify
    InfoListener
    (^void onSuccess [_this ^Map result-map]
      (p/resolve! op-future (into {} result-map)))
    (^void onFailure [_this ^AerospikeException ex]
      (p/reject! op-future ex))))

(defn ^RecordListener reify-record-listener [op-future]
  (reify RecordListener
    (^void onFailure [_this ^AerospikeException ex]
      (p/reject! op-future ex))
    (^void onSuccess [_this ^Key _k ^Record record]
      (p/resolve! op-future record))))

(defn ^RecordSequenceListener reify-record-sequence-listener [op-future callback]
  (reify RecordSequenceListener
    (^void onRecord [_this ^Key k ^Record record]
      (when (= :abort-scan (callback (.userKey k) (record/record->map record)))
        (throw (AerospikeException$QueryTerminated.))))
    (^void onSuccess [_this]
      (p/resolve! op-future true))
    (^void onFailure [_this ^AerospikeException exception]
      (if (instance? AerospikeException$QueryTerminated exception)
        (p/resolve! op-future false)
        (p/reject! op-future exception)))))

(defn ^BatchListListener reify-record-batch-list-listener [op-future]
  (reify BatchListListener
    (^void onFailure [_this ^AerospikeException ex]
      (p/reject! op-future ex))
    (^void onSuccess [_this ^List records]
      (p/resolve! op-future records))))

(defn ^ExistsArrayListener reify-exists-array-listener [op-future]
  (reify ExistsArrayListener
    (^void onFailure [_this ^AerospikeException ex]
      (p/reject! op-future ex))
    (^void onSuccess [_this ^"[Lcom.aerospike.client.Key;" _keys ^"[Z" exists]
      (p/resolve! op-future exists))))


