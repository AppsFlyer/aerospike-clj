(ns aerospike-clj.collections
  (:import (java.util ArrayList Collection Collections List)
           (java.util.function Consumer)))

(defn ->list
  "Returns a new [java.util.List] containing the result of applying `mapper-fn` to each item in `col`.
   Returns an unmodifiable list.
   *Note*: This will usually be faster than `(mapv mapper-fn col)` because:
           - This function allocates a new [java.util.List] in the exactly `(.size col)` size and then
             fills it with the mapped values.
           - If the underlying collection is not a Clojure sequence, then `mapv` will first convert it
             to a Clojure sequence and then map over it. This function will not do that."
  ^List [mapper-fn ^Collection col]
  (let [res (ArrayList. (.size col))]
    (.forEach col
              (reify Consumer
                (accept [_ item]
                  (.add res (mapper-fn item)))))
    (Collections/unmodifiableList res)))
