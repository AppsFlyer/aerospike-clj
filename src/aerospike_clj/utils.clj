(ns aerospike-clj.utils)

(defn- hosts->cluster [hosts]
  (or
    (get (clojure.string/split (first hosts) #"-") 2)
    (get (clojure.string/split (first hosts) #"-|\.") 1)
    (first hosts)))

(defn cluster-name [hosts]
  (-> (hosts->cluster hosts)
      (clojure.string/split  #"\.")
      first))
