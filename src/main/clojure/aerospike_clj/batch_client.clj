(ns aerospike-clj.batch-client
  (:require [aerospike-clj.aerospike-record :as record]
            [aerospike-clj.client :as client]
            [aerospike-clj.collections :as collections]
            [aerospike-clj.protocols :as pt]
            [promesa.core :as p])
  (:import (aerospike_clj.client SimpleAerospikeClient)
           (com.aerospike.client AerospikeClient AerospikeException BatchRecord)
           (com.aerospike.client.async EventLoop EventLoops)
           (com.aerospike.client.listener BatchOperateListListener)
           (com.aerospike.client.policy BatchPolicy)
           (java.util List)))

(deftype ^:private AsyncBatchOperateListListener [op-future]
  BatchOperateListListener
  (^void onSuccess [_this ^List records ^boolean _status]
    (p/resolve! op-future records))
  (^void onFailure [_this ^AerospikeException ex]
    (p/reject! op-future ex)))

(defn- batch-record->map [^BatchRecord batch-record]
  (let [k (.key batch-record)]
    (-> (record/record->map (.record batch-record))
        (assoc :index (.toString (.userKey k)))
        (assoc :set (.setName k))
        (assoc :result-code (.resultCode batch-record)))))

(extend-type SimpleAerospikeClient
  pt/AerospikeBatchOps
  (batch-operate
    ([this batch-records]
     (pt/batch-operate this batch-records {}))
    ([this batch-records conf]
     (let [op-future  (p/deferred)
           policy     (:policy conf)
           batch-list (if (list? batch-records)
                        batch-records
                        (into [] batch-records))
           start-time (System/nanoTime)
           transcoder (:transcoder conf identity)]
       (.operate ^AerospikeClient (.-client this)
                 ^EventLoop (.next ^EventLoops (.-el this))
                 ^BatchOperateListListener (AsyncBatchOperateListListener. op-future)
                 ^BatchPolicy policy
                 ^List batch-list)
       (-> op-future
           (p/then' (comp transcoder #(collections/->list batch-record->map %)) (.-completion-executor this))
           (client/register-events (.-client-events this) :batch-operate nil start-time conf))))))
