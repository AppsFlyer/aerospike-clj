(ns aerospike-clj.bins-test
  (:require [aerospike-clj.bins :as bins]
            [clojure.test :refer [deftest is testing]]))

(deftest remove-nil-bins
  (testing "top-level nil bins are removed"
    (is (= {"a" 1} (bins/remove-nil-bins {"a" 1 "b" nil}))))
  (testing "nested nils and non-map values are kept"
    (is (= {"a" {"x" nil} "b" [nil]} (bins/remove-nil-bins {"a" {"x" nil} "b" [nil]})))
    (is (= 1 (bins/remove-nil-bins 1)))
    (is (= {} (bins/remove-nil-bins {}))))
  (testing "throws when no bins would be left"
    (is (thrown? IllegalArgumentException (bins/remove-nil-bins nil)))
    (is (thrown? IllegalArgumentException (bins/remove-nil-bins {"a" nil "b" nil})))))
