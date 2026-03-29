(ns aerospike-clj.client-test
  (:require [clojure.test :refer [deftest is testing]]
            [aerospike-clj.client :as client])
  (:import (com.aerospike.client.async NioEventLoops NettyEventLoops)))

(deftest create-event-loops-test
  (testing "defaults to NioEventLoops when :event-loop-type is not specified"
    (let [el (client/create-event-loops {})]
      (try
        (is (instance? NioEventLoops el))
        (finally
          (.close el)))))

  (testing "creates NettyEventLoops when :event-loop-type is :NettyEventLoops"
    (let [el (client/create-event-loops {:event-loop-type :NettyEventLoops})]
      (try
        (is (instance? NettyEventLoops el))
        (finally
          (.close el)))))

  (testing "throws on unsupported :event-loop-type"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Unsupported :event-loop-type"
                          (client/create-event-loops {:event-loop-type :InvalidType})))))
