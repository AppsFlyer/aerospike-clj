(ns aerospike-clj.integration.aerospike-setup
  (:require [clj-test-containers.core :as tc]))

(def ^:private ^:const container-port 3000)
(def ^:dynamic *container* nil)

(defn get-host-and-port []
  (format "%s:%s" (:host *container*) (get (:mapped-ports *container*) container-port)))

(defn- start-container []
  (->> {:image-name    "aerospike:ee-6.2.0.3"
        :exposed-ports [container-port]
        :wait-for      {:wait-strategy   :log
                        :message         "objects: all 0 master 0 prole 0 non-replica 0"
                        :times           1
                        :startup-timeout 15}}
       tc/create
       tc/start!))

(defn- stop! []
  (tc/stop! *container*))

(defn with-aerospike [test-fn]
  (binding [*container* (start-container)]
    (try
      (test-fn)
      (finally
        (stop!)))))
