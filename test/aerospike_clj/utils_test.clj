(ns aerospike-clj.utils-test
  (:require [clojure.test :refer [deftest is]]
            [aerospike-clj.utils :as utils]))
  
(deftest vectorize
  (is (= [1] (utils/vectorize 1)))
  (is (= [] (utils/vectorize nil)))
  (is (= [1 2 3] (utils/vectorize '(1 2 3))))
  (is (= [1 2 3] (utils/vectorize [1 2 3])))
  (is (thrown? IllegalArgumentException (utils/vectorize #{1 2 3})))
  (is (thrown? IllegalArgumentException (utils/vectorize {1 2}))))
