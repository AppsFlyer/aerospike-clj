(ns aerospike-clj.policy-test
  (:require [clojure.test :refer [deftest is]]
            [aerospike-clj.policy :as policy])
  (:import (com.aerospike.client.async EventPolicy)
           (com.aerospike.client.policy ReadModeAP ReadModeSC Replica)))

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

(deftest get-health-policy-with-overrides
  (let [base-conf      {"ReadModeAP"          "ALL"
                        "ReadModeSC"          "SESSION"
                        "maxRetries"          5
                        "Replica"             "RANDOM"
                        "sendKey"             true
                        "sleepBetweenRetries"  100
                        "socketTimeout"       3000
                        "timeoutDelay"        4000
                        "totalTimeout"        2000}
        base-policy    (policy/map->policy base-conf)
        health-policy  (policy/map->health-policy
                        base-policy
                        {"totalTimeout" 10000
                         "sendKey"      false})]
    (is (= 2000 (.totalTimeout base-policy)))
    (is (= true (.sendKey base-policy)))
    (is (= 5 (.maxRetries base-policy)))
    (is (= 10000 (.totalTimeout health-policy)))
    (is (= false (.sendKey health-policy)))
    (is (= 5 (.maxRetries health-policy)))
    (is (= ReadModeAP/ALL (.readModeAP health-policy)))
    (is (= ReadModeSC/SESSION (.readModeSC health-policy)))
    (is (= Replica/RANDOM (.replica health-policy)))
    (is (= 100 (.sleepBetweenRetries health-policy)))
    (is (= 3000 (.socketTimeout health-policy)))
    (is (= 4000 (.timeoutDelay health-policy)))
    (is (= 5 (.maxRetries health-policy)))))
