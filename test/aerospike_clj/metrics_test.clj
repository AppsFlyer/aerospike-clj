(ns ^{:author "Ido Barkan"}
  aerospike-clj.metrics-test
  (:require [clojure.test :refer [deftest is]]
            [aerospike-clj.client :as client]))

(deftest get-cluster-stats
  (let [c (client/init-simple-aerospike-client ["localhost"] "test")]
    (is (= [[["event-loops.0.in-process" 0]
             ["event-loops.0.in-queue" 0]
             ["threads-in-use" 0]
             ["nodes.127-0-0-1.sync.in-pool" 0]
             ["nodes.127-0-0-1.sync.in-use" 0]
             ["nodes.127-0-0-1.async.in-pool" 0]
             ["nodes.127-0-0-1.async.in-use" 0]]]
           (client/get-cluster-stats c)))
    (client/stop-aerospike-clients c)))

