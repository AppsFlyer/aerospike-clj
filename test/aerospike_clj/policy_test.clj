(ns aerospike-clj.policy-test
  (:require [clojure.test :refer [deftest is]]
            [aerospike-clj.policy :as policy])
  (:import [com.aerospike.client.async EventPolicy]))

(defn- verify-event-policy-properties
  ([^Exception event-policy]
   (verify-event-policy-properties event-policy {}))

  ([^EventPolicy event-policy conf]
   (is (= (get conf "maxCommandsInProcess" 0) (.maxCommandsInProcess event-policy)))
   (is (= (get conf "maxCommandsInQueue" 0) (.maxCommandsInQueue event-policy)))
   (is (= (get conf "queueInitialCapacity" 256) (.queueInitialCapacity event-policy)))
   (is (= (get conf "minTimeout" 100) (.minTimeout event-policy)))
   (is (= (get conf "ticksPerWheel" 256) (.ticksPerWheel event-policy)))
   (is (= (get conf "commandsPerEventLoop" 256) (.commandsPerEventLoop event-policy)))))

(deftest get-valid-event-policy-with-default-properties
  (let [event-policy (policy/map->event-policy)]
    (verify-event-policy-properties event-policy)))

(deftest get-valid-event-policy-with-max-commands-process-and-queue
  (let [conf {"maxCommandsInProcess" 100 "maxCommandsInQueue" 1}
        event-policy (policy/map->event-policy conf)]
    (verify-event-policy-properties event-policy conf)))

(deftest invalid-event-policy-exception
  (let [conf {"maxCommandsInProcess" 100 "maxCommandsInQueue" 0}]
    (is (thrown-with-msg?
          Exception
          #"setting maxCommandsInProcess>0 and maxCommandsInQueue=0 creates an unbounded delay queue"
          (policy/map->event-policy conf)))))