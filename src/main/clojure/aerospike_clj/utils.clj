(ns aerospike-clj.utils
  (:require [clojure.set :as set])
  (:import (java.util Collection)))

(def ^:private boolean-replacements
  "For bins, Aerospike converts `true` to `1`, `false` to `0` and `nil` values are
  not accepted. The conversion happens only at the top-level of the bin and does not
  seem to affect nested structures. To store these values in the database, they are
  converted to keywords."
  {true  :true
   false :false
   nil   :nil})

(def ^:private reverse-boolean-replacements (set/map-invert boolean-replacements))

;; predicates
(defn single-bin?
  "Predicate function to determine whether data will be stored as a single bin or
  multiple bin record."
  [bin-names]
  (= bin-names [""]))

(defn string-keys?
  "Predicate function to determine whether all keys provided for bins are strings."
  [bin-names]
  (every? string? bin-names))

(defn sanitize-bin-value
  "Values in nested structures are unaffected and do not need to be sanitized. If,
  however, `true`, `false` or `nil` exist as the only value in a bin, they need to
  be sanitized."
  [bin-value]
  (get boolean-replacements bin-value bin-value))

(defn desanitize-bin-value
  "Converts sanitized (keywordized) bin values back to their original value."
  [bin-value]
  (get reverse-boolean-replacements bin-value bin-value))

(defn v->array
  "An optimized way to convert vectors into Java arrays of type `clazz`."
  ([clazz ^Collection v]
   (.toArray v ^"[Ljava.lang.Object;" (make-array clazz 0)))
  ([clazz ^Collection v mapper-fn]
   (let [size     (.size v)
         res      (make-array clazz size)
         iterator (.iterator v)]
     (loop [i (int 0)]
       (when (and (< i size)
                  (.hasNext iterator))
         (aset res i (mapper-fn (.next iterator)))
         (recur (unchecked-inc-int i))))
     res)))

(defn vectorize
  "convert a single value to a vector or any collection to the equivalent vector.
  NOTE: a map or a set have no defined order so vectorize them is not allowed"
  [v]
  (cond
    (or (map? v) (set? v)) (throw (IllegalArgumentException. "undefined sequence order for argument"))
    (or (nil? v) (vector? v) (seq? v)) (vec v)
    :else [v]))
