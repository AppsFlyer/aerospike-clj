(ns ^{:author      "Ido Barkan"
      :integration true}
  aerospike-clj.integration.metrics-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [aerospike-clj.client :as client]
            [aerospike-clj.integration.aerospike-setup :as as-setup]
            [aerospike-clj.protocols :as pt]))

(use-fixtures :once as-setup/with-aerospike)

(deftest get-cluster-stats
  (let [c           (client/init-simple-aerospike-client [(as-setup/get-host-and-port)] "test")
        loopback-v4 "127-0-0-1"
        loopback-v6 "0:0:0:0:0:0:0:1"]
    (is (or
          (= [["event-loops.0.in-process" 0]
              ["event-loops.0.in-queue" 0]
              ["threads-in-use" 0]
              [(format "nodes.%s.sync.in-pool" loopback-v4) 0]
              [(format "nodes.%s.sync.in-use" loopback-v4) 0]
              [(format "nodes.%s.async.in-pool" loopback-v4) 0]
              [(format "nodes.%s.async.in-use" loopback-v4) 0]]
             (pt/get-cluster-stats c))
          (= [["event-loops.0.in-process" 0]
              ["event-loops.0.in-queue" 0]
              ["threads-in-use" 0]
              [(format "nodes.%s.sync.in-pool" loopback-v6) 0]
              [(format "nodes.%s.sync.in-use" loopback-v6) 0]
              [(format "nodes.%s.async.in-pool" loopback-v6) 0]
              [(format "nodes.%s.async.in-use" loopback-v6) 0]]
             (pt/get-cluster-stats c))))
    (pt/stop c)))

