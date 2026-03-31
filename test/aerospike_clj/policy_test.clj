(ns aerospike-clj.policy-test
  (:require [clojure.test :refer [deftest is testing]]
            [aerospike-clj.policy :as policy]
            [aerospike-clj.client :as client])
  (:import (com.aerospike.client.async EventPolicy)
           (com.aerospike.client.policy ClientPolicy Policy ReadModeAP ReadModeSC Replica)
           (aerospike_clj.client SimpleAerospikeClient)))

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

(deftest apply-policy-fields!-sets-fields
  (let [conf {"maxRetries"          5
              "sendKey"             true
              "sleepBetweenRetries" 200
              "socketTimeout"       3000
              "timeoutDelay"        500
              "totalTimeout"        10000
              "ReadModeAP"          "ALL"
              "ReadModeSC"          "ALLOW_REPLICA"
              "Replica"             "MASTER_PROLES"}
        p    (policy/apply-policy-fields! (Policy.) conf)]
    (is (= 5 (.maxRetries p)))
    (is (true? (.sendKey p)))
    (is (= 200 (.sleepBetweenRetries p)))
    (is (= 3000 (.socketTimeout p)))
    (is (= 500 (.timeoutDelay p)))
    (is (= 10000 (.totalTimeout p)))
    (is (= ReadModeAP/ALL (.readModeAP p)))
    (is (= ReadModeSC/ALLOW_REPLICA (.readModeSC p)))
    (is (= Replica/MASTER_PROLES (.replica p)))))

(deftest apply-policy-fields!-ignores-absent-and-nil-keys
  (let [defaults (Policy.)
        p        (policy/apply-policy-fields! (Policy.) {"maxRetries"   nil
                                                         "totalTimeout" nil
                                                         "ReadModeAP"  nil})]
    (is (= (.maxRetries defaults) (.maxRetries p)))
    (is (= (.totalTimeout defaults) (.totalTimeout p)))
    (is (= (.readModeAP defaults) (.readModeAP p)))
    (is (= (.sendKey defaults) (.sendKey p)))
    (is (= (.replica defaults) (.replica p)))))

(deftest apply-policy-fields!-mutates-pre-configured-policy
  (let [base (Policy.)
        _    (set! (.totalTimeout base) 2000)
        _    (set! (.maxRetries base) 3)
        copy (Policy. base)
        p    (policy/apply-policy-fields! copy {"totalTimeout" 9000})]
    (is (= 9000 (.totalTimeout p)))
    (is (= 3 (.maxRetries p)) "non-overridden field keeps the copied value")
    (is (identical? copy p) "returns the same mutated instance")))

(deftest health-policy-custom-flows-to-client
  (let [fake-el     (reify com.aerospike.client.async.EventLoops (close [_]))
        stub-create (fn [_ _ _] nil)]
    (testing "custom :health-policy is stored on SimpleAerospikeClient"
      (let [hp (policy/apply-policy-fields! (Policy.) {"totalTimeout" 5000})
            c  (with-redefs [client/create-event-loops (constantly fake-el)
                             #'aerospike-clj.client/create-client stub-create]
                 (client/init-simple-aerospike-client
                   ["localhost"]
                   "test"
                   {:health-policy hp
                    :client-policy (ClientPolicy.)
                    :event-loops   fake-el}))]
        (is (identical? hp (.-health-policy ^SimpleAerospikeClient c))
            "the exact Policy instance passed as :health-policy is stored on the client")
        (is (= 5000 (.totalTimeout ^Policy (.-health-policy ^SimpleAerospikeClient c)))
            "totalTimeout matches the value configured on the custom health-policy")))

    (testing "default :health-policy is a copy of readPolicyDefault"
      (let [read-policy (policy/map->policy {"totalTimeout" 7777})
            cp          (doto (ClientPolicy.)
                          (-> .-readPolicyDefault (set! read-policy)))
            c           (with-redefs [client/create-event-loops (constantly fake-el)
                                      #'aerospike-clj.client/create-client stub-create]
                          (client/init-simple-aerospike-client
                            ["localhost"]
                            "test"
                            {:client-policy cp
                             :event-loops   fake-el}))]
        (is (= 7777 (.totalTimeout ^Policy (.-health-policy ^SimpleAerospikeClient c)))
            "totalTimeout is inherited from the client-policy's readPolicyDefault")
        (is (not (identical? read-policy (.-health-policy ^SimpleAerospikeClient c)))
            "health-policy is a defensive copy, not the same instance")))))
