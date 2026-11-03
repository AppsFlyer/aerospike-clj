(ns aerospike-clj.mock-client
  (:refer-clojure :exclude [update])
  (:require [promesa.core :as p]
            [aerospike-clj.bins :as bins]
            [aerospike-clj.client :as client]
            [aerospike-clj.protocols :as pt]
            [aerospike-clj.utils])
  (:import (com.aerospike.client AerospikeException ResultCode)
           (com.aerospike.client.policy RecordExistsAction WritePolicy)
           (java.time Clock Instant ZoneOffset)))

(def ^:private DEFAULT_SET "__DEFAULT__")
(def FIXED_CLOCK (Clock/fixed (Instant/parse "2022-01-01T00:00:00.00Z") ZoneOffset/UTC))

(defn expiration->ttl
  "converts an expiration into TTL as seconds from `Aerospike epoch`"
  [expiration]
  (let [now-epoch (.getEpochSecond (Instant/now FIXED_CLOCK))]
    (- (+ now-epoch expiration) client/EPOCH)))

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
   {:payload payload :ttl (expiration->ttl ttl) :gen generation}))

(defn- do-swap [state swap-fn]
  (try
    (swap! state swap-fn)
    (p/resolved true)
    (catch AerospikeException ex
      (p/rejected ex))))

(defn- without-nil-bins [payload]
  (if (map? payload)
    (reduce-kv (fn [m k v] (if (nil? v) (dissoc m k) m)) payload payload)
    payload))

(defn- check-write
  "Transcodes `data` once and checks it the way the real client does for a write with `action`.
  Returns `{:error e}` for a write the real client rejects, otherwise `{:data d :payload p}`, where `d` is the
  transcoded data and `p` is it without `nil` bins. Only top-level values are checked, not values nested in lists
  or maps."
  [data conf action]
  (try
    (let [data ((get-transcoder conf) data)]
      (bins/data->write-bins data action)
      {:data data :payload (without-nil-bins data)})
    (catch Exception e
      {:error e})))

(defn- update-action? [action]
  (or (= RecordExistsAction/UPDATE action)
      (= RecordExistsAction/UPDATE_ONLY action)))

(defn- put-action
  "No `:policy` is `write-policy`'s REPLACE. A `nil` policy is the client's default, which is UPDATE."
  [conf]
  (if (contains? conf :policy)
    (if-let [^WritePolicy policy (:policy conf)]
      (.recordExistsAction policy)
      RecordExistsAction/UPDATE)
    RecordExistsAction/REPLACE))

(defn- remove-bins
  "An UPDATE write whose bins are all `nil` removes those bins, and the record when none are left."
  [state set-name k bin-names expiration]
  (do-swap state (fn [current-state]
                   (let [record (get-record current-state set-name k)]
                     (when-not record
                       (throw (AerospikeException.
                                ResultCode/KEY_NOT_FOUND_ERROR
                                (str "Call to remove the bins of non-existing key '" k "'"))))
                     (let [payload (apply dissoc (:payload record) bin-names)]
                       (if (empty? payload)
                         (delete-record current-state set-name k)
                         (set-record current-state
                                     (assoc record :payload payload :ttl (expiration->ttl expiration))
                                     set-name
                                     k)))))))

(defn- only-nil-bins? [{:keys [data payload]}]
  (and (map? data) (seq data) (empty? payload)))

(defn- put-checked [state k set-name {:keys [data payload] :as checked} action expiration]
  (if (and (update-action? action) (only-nil-bins? checked))
    (remove-bins state set-name k (keys data) expiration)
    (do-swap state (fn [current-state] (set-record current-state (create-record payload expiration) set-name k)))))

(defn- update* [state k set-name payload expected-generation new-expiration]
  (let [swap-fn (fn [current-state]
                  (let [current-generation (get-generation current-state set-name k)]
                    (when-not (= current-generation expected-generation)
                      (throw (AerospikeException.
                               (ResultCode/GENERATION_ERROR)
                               (str "Record 'generation' count does not match expected value: '"
                                    expected-generation "'"))))
                    (set-record current-state
                                (create-record payload new-expiration (inc current-generation))
                                set-name
                                k)))]
    (do-swap state swap-fn)))

(defn- filter-bins [bins record]
  (clojure.core/update record :payload #(select-keys % bins)))


(defrecord MockClient [state]
  pt/AerospikeReadOps
  (get-single [this k set-name] (pt/get-single this k set-name {} nil))

  (get-single [this k set-name conf] (pt/get-single this k set-name conf nil))

  (get-single [_ k set-name conf _]
    (let [transcoder (get-transcoder conf)]
      (p/resolved (transcoder (get-record @state set-name k)))))

  (get-single-no-meta [this k set-name]
    (pt/get-single this k set-name {:transcoder :payload}))

  (get-single-no-meta [this k set-name bin-names]
    (pt/get-single this k set-name {:transcoder :payload} bin-names))

  (exists? [this k set-name]
    (pt/exists? this k set-name {}))

  (exists? [_ k set-name _]
    (p/resolved (record-exists? @state set-name k)))

  (get-batch [this batch-reads]
    (pt/get-batch this batch-reads {}))

  (get-batch [this batch-reads conf]
    (p/resolved
      (mapv
        (fn [record]
          (let [bins     (if (= [:all] (:bins record)) nil (:bins record))
                get-bins (if bins (partial filter-bins bins) identity)]
            (get-bins @(pt/get-single this (:index record) (:set record) conf))))
        batch-reads)))

  (exists-batch [this indices]
    (pt/exists-batch this indices {}))

  (exists-batch [_ indices _]
    (p/resolved
      (mapv (fn [v] (record-exists? @state (:set v) (:index v))) indices)))

  pt/AerospikeWriteOps
  (put [this k set-name data expiration]
    (pt/put this k set-name data expiration {}))

  (put [_ k set-name data expiration conf]
    (let [action  (put-action conf)
          checked (check-write data conf action)]
      (if-let [e (:error checked)]
        (p/rejected e)
        (put-checked state k set-name checked action expiration))))

  (create [this k set-name data expiration]
    (pt/create this k set-name data expiration {}))

  (create [_ k set-name data expiration conf]
    (let [{:keys [error payload]} (check-write data conf RecordExistsAction/CREATE_ONLY)
          swap-fn (fn [current-state]
                    (when (some? (get-record current-state set-name k))
                      (throw (AerospikeException.
                               (ResultCode/KEY_EXISTS_ERROR)
                               (str "Call to `create` on existing key '" k "'"))))
                    (set-record current-state (create-record payload expiration) set-name k))]
      (if error
        (p/rejected error)
        (do-swap state swap-fn))))

  (put-multiple [this indices set-names payloads expiration-seq]
    (pt/put-multiple this indices set-names payloads expiration-seq {}))

  (put-multiple [_ indices set-names payloads expiration-seq conf]
    (let [action  (put-action conf)
          checked (mapv #(check-write % conf action) payloads)]
      (if-let [e (some :error checked)]
        (p/rejected e)
        (p/all
          (map (fn [k set-name c expiration]
                 (put-checked state k set-name c action expiration))
               indices set-names checked expiration-seq)))))

  pt/AerospikeUpdateOps
  (set-single [this k set-name data expiration]
    (pt/set-single this k set-name data expiration {}))

  (set-single [_ k set-name data expiration conf]
    (let [{:keys [error data payload] :as checked} (check-write data conf RecordExistsAction/UPDATE)]
      (cond
        error (p/rejected error)
        (only-nil-bins? checked) (remove-bins state set-name k (keys data) expiration)
        :else (update* state k set-name payload (get-generation @state set-name k) expiration))))

  (add-bins [this k set-name new-data new-expiration]
    (pt/add-bins this k set-name new-data new-expiration {}))

  (add-bins [_ k set-name new-data new-expiration conf]
    (let [{:keys [error data]} (check-write new-data conf RecordExistsAction/UPDATE_ONLY)
          swap-fn (fn [current-state]
                    (if-let [old-data (:payload (get-record current-state set-name k))]
                      (let [merged-data (without-nil-bins (merge old-data data))
                            generation  (get-generation current-state set-name k)]
                        (if (empty? merged-data)
                          (delete-record current-state set-name k)
                          (set-record current-state (create-record merged-data new-expiration generation) set-name k)))
                      (throw (AerospikeException.
                               (ResultCode/KEY_NOT_FOUND_ERROR)
                               (str "Call to `add-bins` on non-existing key '" k "'")))))]
      (if error
        (p/rejected error)
        (do-swap state swap-fn))))

  (replace-only [this k set-name data expiration]
    (pt/replace-only this k set-name data expiration {}))

  (replace-only [_ k set-name data expiration conf]
    (let [{:keys [error payload]} (check-write data conf RecordExistsAction/REPLACE_ONLY)
          swap-fn (fn [current-state]
                    (if (some? (get-record current-state set-name k))
                      (set-record current-state (create-record payload expiration) set-name k)
                      (throw (AerospikeException.
                               ResultCode/KEY_NOT_FOUND_ERROR
                               (str "Call to `replace-only` on non-existing key '" k "'")))))]
      (if error
        (p/rejected error)
        (do-swap state swap-fn))))

  (update [this k set-name new-record generation new-expiration]
    (pt/update this k set-name new-record generation new-expiration {}))

  (update [_ k set-name new-record expected-generation new-expiration conf]
    (let [{:keys [error payload]} (check-write new-record conf RecordExistsAction/REPLACE)]
      (if error
        (p/rejected error)
        (update* state k set-name payload expected-generation new-expiration))))

  (touch [_ k set-name expiration]
    (let [swap-fn (fn [current-state]
                    (if-let [record (get-record current-state set-name k)]
                      (let [new-record (assoc record :ttl (expiration->ttl expiration))]
                        (set-record current-state new-record set-name k))
                      (throw (AerospikeException.
                               ResultCode/KEY_NOT_FOUND_ERROR
                               (str "Call to `touch` on non-existing key '" k "'")))))]
      (do-swap state swap-fn)))

  pt/AerospikeSingleIndexBatchOps
  (operate [this k set-name expiration operations]
    (pt/operate this k set-name expiration operations nil))

  (operate [_ _ _ _ _ _]
    (throw (RuntimeException. "Function not implemented")))

  pt/AerospikeSetOps
  (scan-set [_this _ set-name conf]
    (let [state    (get @state set-name)
          callback (:callback conf)
          bins     (:bins conf)
          get-bins (if bins (partial filter-bins bins) identity)]

      (p/resolved
        (try
          (doseq [[k v] state]
            (when (= (callback k (get-bins v)) :abort-scan)
              (throw (ex-info "" {:abort-scan true}))))
          true
          (catch Exception ex
            (if (:abort-scan (ex-data ex))
              false
              (throw ex)))))))

  pt/AerospikeDeleteOps
  (delete [_ k set-name]
    (let [success? (atom nil)
          swap-fn  (fn [current-state]
                     (if (record-exists? current-state set-name k)
                       (do
                         (reset! success? true)
                         (delete-record current-state set-name k))
                       (do
                         (reset! success? false)
                         current-state)))]
      (do-swap state swap-fn)
      (p/resolved @success?)))

  (delete-bins [this k set-name bin-names new-expiration]
    (pt/delete-bins this k set-name bin-names new-expiration {}))

  (delete-bins [_ k set-name bin-names new-expiration _]
    (let [swap-fn (fn [current-state]
                    (if-let [old-data (:payload (get-record current-state set-name k))]
                      (let [merged-data (apply dissoc old-data bin-names)
                            generation  (get-generation current-state set-name k)
                            new-record  (create-record merged-data new-expiration generation)]
                        (set-record current-state new-record set-name k))
                      (throw (AerospikeException.
                               (ResultCode/KEY_NOT_FOUND_ERROR)
                               (str "Call to `delete-bins` on non-existing key '" k "'")))))]
      (do-swap state swap-fn)))

  pt/AerospikeAdminOps
  (healthy? [_] true)

  (healthy? [_ _] true)

  (get-cluster-stats [_] [[]])

  (stop [_]
    (reset! state {DEFAULT_SET {}})))

(defprotocol Stateful
  (get-state [this] [this set-name]))

(extend-type MockClient
  Stateful
  (get-state
    ([this]
     (get-state this DEFAULT_SET))
    ([this set-name]
     (get @(:state this) set-name))))

(defn create-instance [& _]
  (MockClient. (atom {DEFAULT_SET {}})))
