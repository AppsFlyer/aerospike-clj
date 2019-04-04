(ns aerospike-clj.utils
  (:require [clojure.set :as set]
            [clojure.string :as s]))

(defn- hosts->cluster [hosts]
  (or
    (get (s/split (first hosts) #"-") 2)
    (get (s/split (first hosts) #"-|\.") 1)
    (first hosts)))

(defn cluster-name [hosts]
  (-> (hosts->cluster hosts)
      (s/split  #"\.")
      first))

(def ^:private boolean-replacements
  "For bins, Aerospike converts `true` to `1`, `false` to `0` and `nil` values are
  not accepted. The conversion happens only at the top-level of the bin and does not
  seem to affect nested structures. To store these values in the database, they are
  converted to keywords."
  {true  :true
   false :false
   nil   :nil})

;; transducers
(def ^:private x-sanitize
  (replace boolean-replacements))

(def ^:private x-desanitize
  (replace (set/map-invert boolean-replacements)))

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
  (->> (into [] x-sanitize (vector bin-value))
       first))

(defn desanitize-bin-value
  "Converts sanitized (keywordized) bin values back to their original value."
  [bin-value]
  (->> (into [] x-desanitize (vector bin-value))
       first))
