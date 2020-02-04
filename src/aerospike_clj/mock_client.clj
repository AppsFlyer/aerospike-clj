(ns aerospike-clj.mock-client
  (:refer-clojure :exclude [update])
  (:require [aerospike-clj.client :as client]
            [aerospike-clj.utils]
            [manifold.deferred :as defer]
            [clojure.pprint :refer [pprint]])
  (:import (clojure.lang IPersistentVector IPersistentMap)
           (com.aerospike.client AerospikeException ResultCode)))


(defprotocol AerospikeClient
  (get-single [this k set-name]
    [this k set-name conf]
    [this k set-name conf bin-names])
  (get-multiple [this indices sets]
    [this indices sets conf])
  (exists? [this k set-name]
    [this k set-name conf])
  (get-single-no-meta [this k set-name]
    [this k set-name ^IPersistentVector bin-names])
  (put [this k set-name data expiration]
    [this k set-name data expiration conf])
  (add-bins [this k set-name ^IPersistentMap new-data new-expiration]
    [this k set-name ^IPersistentMap new-data new-expiration conf])
  (put-multiple [this indices set-names payloads expiration-seq]
    [this indices set-names payloads expiration-seq conf])
  (create [this k set-name data expiration]
    [this k set-name data expiration conf])
  (replace-only [this k set-name data expiration]
    [this k set-name data expiration conf])
  (update [this k set-name new-record generation new-expiration]
    [this k set-name new-record generation new-expiration conf])
  (set-single [this k set-name data expiration]
    [this k set-name data expiration conf])
  (touch [this k set-name expiration])
  (delete [this k set-name]
    [this k set-name conf])
  (delete-bins [this k set-name ^IPersistentVector bin-names new-expiration]
    [this k set-name ^IPersistentVector bin-names new-expiration conf])
  (operate [this k set-name expiration operations]
    [this k set-name expiration operations conf])
  (get-cluster-stats [this])
  (healthy? [this operation-timeout-ms])
  (stop-aerospike-client [this])
  (get-state [this] [this set-name])
  (print-state [this]))



(def ^:private DEFAULT_SET "__DEFAULT__")

(defn- get-set-name [set-name]
  (if (some? set-name) set-name DEFAULT_SET))

(defn- get-record [state set-name k]
  (get-in state [(get-set-name set-name) k]))

(defn- set-record [state value set-name k]
  (assoc-in state [(get-set-name set-name) k] value))

(defn- delete-record [state set-name k]
  (clojure.core/update state (get-set-name set-name) dissoc k))

(defn record-exists? [state set-name k]
  (some? (get-record state (get-set-name set-name) k)))

(defn- get-generation
  "The number of times the record has been modified.
  Defaults to 0 if the record doesn't exist or the value cannot be determined."
  [state set-name k]
  (:gen (get-record state set-name k) 0))

(defn- get-transcoder [conf]
  (:transcoder conf identity))

(defn- create-record
  ([payload ttl] (create-record payload ttl 1))
  ([payload ttl generation]
   {:payload payload :ttl ttl :gen generation}))

(defn- do-swap [state swap-fn]
  (try
    (do
      (swap! state swap-fn)
      (defer/success-deferred true))
    (catch AerospikeException ex
      (defer/error-deferred ex))))


(defrecord MockClient [state]
  AerospikeClient
  (get-single [this k set-name] (get-single this k set-name {} nil))

  (get-single [this k set-name conf] (get-single this k set-name conf nil))

  (get-single [_ k set-name conf _]
    (let [transcoder (get-transcoder conf)]
      (defer/success-deferred (transcoder (get-record @state set-name k)))))

  (get-multiple [this indices set-names]
    (get-multiple this indices set-names {}))

  (get-multiple [this indices set-names conf]
    (defer/success-deferred
      (mapv (fn [k set-name] @(get-single this k set-name conf)) indices set-names)))

  (get-single-no-meta [this k set-name]
    (get-single this k set-name {:transcoder :payload}))

  (get-single-no-meta [this k set-name bin-names]
    (get-single this k set-name {:transcoder :payload} bin-names))

  (exists? [this k set-name]
    (exists? this k set-name {}))

  (exists? [_ k set-name _]
    (defer/success-deferred (record-exists? @state set-name k)))

  (put [this k set-name data expiration]
    (put this k set-name data expiration {}))

  (put [_ k set-name data expiration conf]
    (let [transcoder (get-transcoder conf)
          new-record (create-record (transcoder data) expiration)
          swap-fn (fn [current-state] (set-record current-state new-record set-name k))]
      (do-swap state swap-fn)))

  (set-single [this k set-name data expiration]
    (set-single this k set-name data expiration {}))

  (set-single [this k set-name data expiration conf]
    (let [generation (get-generation @state set-name k)]
      (update this k set-name data generation expiration conf)))

  (add-bins [this k set-name new-data new-expiration]
    (add-bins this k set-name new-data new-expiration {}))

  (add-bins [_ k set-name new-data new-expiration conf]
    (let [transcoder (get-transcoder conf)
          swap-fn (fn [current-state]
                    (if-let [old-data (:payload (get-record current-state set-name k))]
                      (let [merged-data (merge old-data new-data)
                            generation (get-generation current-state set-name k)
                            new-record (create-record
                                         (transcoder merged-data) new-expiration generation)]
                        (set-record current-state new-record set-name k))
                      (throw (AerospikeException.
                               (ResultCode/KEY_NOT_FOUND_ERROR)
                               (str "Call to `add-bins` on non-existing key '" k "'")))))]
      (do-swap state swap-fn)))

  (put-multiple [this indices set-names payloads expiration-seq]
    (put-multiple this indices set-names payloads expiration-seq {}))

  (put-multiple [this indices set-names payloads expiration-seq conf]
    (defer/success-deferred
      (mapv (fn [k set-name payload expiration]
              @(put this k set-name payload expiration conf))
            indices set-names payloads expiration-seq)))

  (create [this k set-name data expiration]
    (create this k set-name data expiration {}))

  (create [_ k set-name data expiration conf]
    (let [swap-fn (fn [current-state]
                    (when (some? (get-record current-state set-name k))
                      (throw (AerospikeException.
                               (ResultCode/KEY_EXISTS_ERROR)
                               (str "Call to `create` on existing key '" k "'"))))

                    (let [transcoder (get-transcoder conf)
                          new-record (create-record (transcoder data) expiration)]
                      (set-record current-state new-record set-name k)))]
      (do-swap state swap-fn)))

  (replace-only [this k set-name data expiration]
    (replace-only this k set-name data expiration {}))

  (replace-only [_ k set-name data expiration conf]
    (let [swap-fn (fn [current-state]
                    (if (some? (get-record current-state set-name k))
                      (let [transcoder (get-transcoder conf)
                            new-record (create-record (transcoder data) expiration)]
                        (set-record current-state new-record set-name k))
                      (AerospikeException.
                        ResultCode/KEY_NOT_FOUND_ERROR
                        (str "Call to `replace-only` on non-existing key '" k "'"))))]
      (do-swap state swap-fn)))

  (update [this k set-name new-record generation new-expiration]
    (update this k set-name new-record generation new-expiration {}))

  (update [_ k set-name new-record expected-generation new-expiration conf]
    (let [swap-fn (fn [current-state]
                    (let [current-generation (get-generation current-state set-name k)]
                      (when-not (= current-generation expected-generation)
                        (throw (AerospikeException.
                                 (ResultCode/GENERATION_ERROR)
                                 (str "Record 'generation' count does not match expected value: '"
                                      expected-generation "'"))))

                      (let [transcoder (get-transcoder conf)
                            new-record (create-record
                                         (transcoder new-record) new-expiration (inc current-generation))]
                        (set-record current-state new-record set-name k))))]
      (do-swap state swap-fn)))

  (touch [_ k set-name expiration]
    (let [swap-fn (fn [current-state]
                    (if-let [record (get-record current-state set-name k)]
                      (let [new-record (assoc record :ttl expiration)]
                        (set-record current-state new-record set-name k))
                      (throw (AerospikeException.
                               ResultCode/KEY_NOT_FOUND_ERROR
                               (str "Call to `touch` on non-existing key '" k "'")))))]
      (do-swap state swap-fn)))

  (delete [_ k set-name]
    (let [success? (atom nil)
          swap-fn (fn [current-state]
                    (if (record-exists? current-state set-name k)
                      (do
                        (reset! success? true)
                        (delete-record current-state set-name k))
                      (do
                        (reset! success? false)
                        current-state)))]
      (do-swap state swap-fn)
      (defer/success-deferred @success?)))

  (delete-bins [this k set-name bin-names new-expiration]
    (delete-bins this k set-name bin-names new-expiration {}))

  (delete-bins [_ k set-name bin-names new-expiration _]
    (let [swap-fn (fn [current-state]
                    (if-let [old-data (:payload (get-record current-state set-name k))]
                      (let [merged-data (apply dissoc old-data bin-names)
                            generation (get-generation current-state set-name k)
                            new-record (create-record merged-data new-expiration generation)]
                        (set-record current-state new-record set-name k))
                      (throw (AerospikeException.
                               (ResultCode/KEY_NOT_FOUND_ERROR)
                               (str "Call to `delete-bins` on non-existing key '" k "'")))))]
      (do-swap state swap-fn)))

  (operate [this k set-name expiration operations]
    (operate this k set-name expiration operations nil))

  (operate [_ _ _ _ _ _]
    (throw (RuntimeException. "Function not implemented")))

  (healthy? [_ _] true)

  (get-cluster-stats [_] [[]])

  (stop-aerospike-client [_]
    (reset! state {DEFAULT_SET {}}))

  (get-state [this]
    (get-state this DEFAULT_SET))

  (get-state [_ set-name]
    (get @state set-name))

  (print-state [_]
    (pprint @state)))

(defn expiry-unix [ttl]
  (client/expiry-unix ttl))

(defn create-instance [& _]
  (MockClient. (atom {DEFAULT_SET {}})))

(defn init-mock [test-fn]
  (with-redefs
    [client/init-simple-aerospike-client create-instance
     client/stop-aerospike-client stop-aerospike-client
     client/get-single get-single
     client/get-multiple get-multiple
     client/exists? exists?
     client/get-single-no-meta get-single-no-meta
     client/put put
     client/add-bins add-bins
     client/put-multiple put-multiple
     client/create create
     client/replace-only replace-only
     client/update update
     client/set-single set-single
     client/touch touch
     client/delete delete
     client/delete-bins delete-bins
     client/operate operate
     client/get-cluster-stats get-cluster-stats
     client/healthy? healthy?
     client/expiry-unix expiry-unix]
    (test-fn)))