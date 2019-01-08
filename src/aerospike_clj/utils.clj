(ns aerospike-clj.utils
  (:require [clojure.string :as s]))

(defn- hosts->cluster [hosts]
  (or
    (get (s/split (first hosts) #"-") 2)
    (get (s/split (first hosts) #"-|\.") 1)
    (first hosts)))

(defn cluster-name [hosts]
  (-> (hosts->cluster hosts)
      (s/split  #"\.")
      first))
