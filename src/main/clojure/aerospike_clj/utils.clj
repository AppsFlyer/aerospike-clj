(ns aerospike-clj.utils
  (:import (java.util Collection)))

;; predicates
(defn single-bin?
  "Predicate function to determine whether data will be stored as a single bin or
  multiple bin record."
  [bin-names]
  (= bin-names [""]))

(defn string-keys?
  {:doc        "Predicate function to determine whether all keys provided for bins are strings."
   :deprecated "3.1.0"}
  [bin-names]
  (every? string? bin-names))

(defn v->array
  "An optimized way to convert [[java.util.Collection]]s into Java arrays of type `clazz`."
  ([clazz ^Collection v]
   (.toArray v ^"[Ljava.lang.Object;" (make-array clazz 0)))
  ([clazz mapper-fn ^Collection v]
   (let [size     (.size v)
         res      ^"[Ljava.lang.Object;" (make-array clazz size)
         iterator (.iterator v)]
     (loop [i 0]
       (when (and (< i size)
                  (.hasNext iterator))
         (aset res i (mapper-fn (.next iterator)))
         (recur (inc i))))
     res)))

(defn vectorize
  "convert a single value to a vector or any collection to the equivalent vector.
  NOTE: a map or a set have no defined order so vectorize them is not allowed"
  [v]
  (cond
    (or (map? v) (set? v)) (throw (IllegalArgumentException. "undefined sequence order for argument"))
    (or (nil? v) (vector? v) (seq? v)) (vec v)
    :else [v]))
