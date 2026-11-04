(ns aerospike-clj.bins-test
  (:require [aerospike-clj.bins :as bins]
            [clojure.test :refer [deftest is testing]])
  (:import (com.aerospike.client Bin Value$NullValue)))

(defn- bins->map [bins]
  (into {} (map (fn [^Bin b] [(.-name b) (.getObject (.-value b))])) bins))

(deftest data->bins
  (testing "nil bins remove the bin, unless omitted"
    (let [bins (bins/data->bins {"a" 1 "b" nil})]
      (is (= 2 (count bins)))
      (is (instance? Value$NullValue (.-value ^Bin (some #(when (= "b" (.-name ^Bin %)) %) bins)))))
    (is (= {"a" 1} (bins->map (bins/data->bins {"a" 1 "b" nil} true true)))))
  (testing "nested nils and single-bin values are kept"
    (is (= {"a" {"x" nil} "b" [nil]} (bins->map (bins/data->bins {"a" {"x" nil} "b" [nil]} true true))))
    (is (= {"" 1} (bins->map (bins/data->bins 1 true true))))
    (is (empty? (bins/data->bins {} true true))))
  (testing "nil data always throws"
    (is (thrown? IllegalArgumentException (bins/data->bins nil)))
    (is (thrown? IllegalArgumentException (bins/data->bins nil true true))))
  (testing "only nil bins throw when a value is required"
    (is (thrown? IllegalArgumentException (bins/data->bins {"a" nil "b" nil} true true)))
    (is (thrown? IllegalArgumentException (bins/data->bins {"a" nil} false true)))
    (is (= 1 (count (bins/data->bins {"a" nil})))))
  (testing "bin names are checked before nil bins are omitted"
    (is (thrown-with-msg? Exception #"string values as bin names" (bins/data->bins {:a nil "b" 1} true true)))
    (is (thrown-with-msg? Exception #"17 characters" (bins/data->bins {"abcdefghijklmnopq" nil "b" 1} true true))))
  (testing "unsupported values name their type"
    (is (thrown-with-msg? Exception #"Unsupported type: clojure.lang.Keyword" (bins/data->bins {"a" :foo})))
    (is (thrown-with-msg? Exception #"Unsupported type" (bins/data->bins #{1 2})))))
