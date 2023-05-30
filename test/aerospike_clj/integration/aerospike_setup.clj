(ns aerospike-clj.integration.aerospike-setup
  (:require [clj-test-containers.core :as tc]))

(def ^:private ^:const container-port 3000)
(def ^:private container (atom nil))

(defn get-host-and-port []
  (format "%s:%s" (:host @container) (get (:mapped-ports @container) container-port)))

(defn- start-container []
  (->> {:image-name    "aerospike:ee-6.2.0.3"
       :exposed-ports [container-port]
       :wait-for      {:wait-strategy   :log
                       :message         "objects: all 0 master 0 prole 0 non-replica 0"
                       :times           1
                       :startup-timeout 15}}
      tc/create
      tc/start!
      (reset! container)))

(defn- stop! []
  (tc/stop! @container)
  (reset! container nil?))

(defn with-aerospike [test-fn]
  (start-container)
  (try
    (test-fn)
    (finally
      (stop!))))

