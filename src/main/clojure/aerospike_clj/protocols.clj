(ns aerospike-clj.protocols
  (:refer-clojure :exclude [update]))

(defprotocol AerospikeReadOps
  "Read operations of a single Aerospike record."
  (get-single
    [this index set-name]
    [this index set-name conf]
    [this index set-name conf bin-names]
    "Returns a single record: `(transcoder AerospikeRecord)`. The default transcoder is `identity`.
    Pass a `:policy` in `conf` to use a non-default `ReadPolicy`.")

  (get-single-no-meta
    [this index set-name]
    [this index set-name bin-names]
    "Shorthand to return a single record payload only.")

  (exists?
    [this index set-name]
    [this index set-name conf]
    "Test if an index exists."))

(defprotocol AerospikeWriteOps
  "Write operations of a single Aerospike record."
  (put
    [this index set-name data expiration]
    [this index set-name data expiration conf]
    "Writes `data` into a record with the key `index`, with the TTL of `expiration` seconds.
    `index` should be string. Pass a function in `(:transcoder conf)` to modify `data` before it
    is sent to the DB.
    Pass a `WritePolicy` in `(:policy conf)` to uses the non-default policy.
    When a Clojure map is provided for the `data` argument, a multiple bin record will be created.
    Each key-value pair in the map will be treated as a bin-name, bin-value pair. Bin-names must be
    strings. Bin-values can be any nested data structure.")

  (create
    [this index set-name data expiration]
    [this index set-name data expiration conf]
    "`put` with a `create-only` policy."))

(defprotocol AerospikeUpdateOps
  "Update operations of a single Aerospike record."
  (set-single
    [this index set-name data expiration]
    [this index set-name data expiration conf]
    "`put` with an `update` policy.")

  (replace-only
    [this index set-name data expiration]
    [this index set-name data expiration conf]
    "`put` with a `replace-only` policy.")

  (update
    [this index set-name new-record generation new-expiration]
    [this index set-name new-record generation new-expiration conf]
    "Write a new value for the key `index`.
    Generation: the expected modification count of the record (i.e. how many times was it
    modified before my current action). Pass a function in `(:transcoder conf)` to modify
    `data` before it is sent to the DB.")

  (add-bins
    [this index set-name new-data new-expiration]
    [this index set-name new-data new-expiration conf]
    "Add bins to an existing record without modifying old data. The `new-data` must be a
    Clojure map.")

  (touch [this index set-name expiration]
    "Updates the TTL of the record stored under at `index` to `expiration` seconds from now.
    Expects records to exist."))

(defprotocol AerospikeDeleteOps
  "Delete operations of a single Aerospike record."
  (delete
    [this index set-name]
    [this index set-name conf]
    "Delete the record stored at `index`.
    Returns async true/false for deletion success (hit).")

  (delete-bins
    [this index set-name bin-names new-expiration]
    [this index set-name bin-names new-expiration conf]
    "Delete bins from an existing record. The `bin-names` must be a vector of strings."))

(defprotocol AerospikeBatchOps
  "Various operations of batches of Aerospike records."
  (get-batch
    [this batch-reads]
    [this batch-reads conf]
    "Get a batch of records from the cluster asynchronously. `batch-reads` is a collection of maps
    of the form `{:index \"foo\" :set \"someset\" :bins [...]}`. the `:bins` key can have required
    bins for the batched keys or missing/[:all] to get all the bins (see `_get`). The result is a
    vector of `AerospikeRecord` in the same order of keys. Missing keys result in `nil` in corresponding
    positions.")

  (put-multiple
    [this indices set-names payloads expirations]
    [this indices set-names payloads expirations conf]
    "Put multiple payloads by invoking `put`. All arguments should be mutually
    corresponding sequences.")

  (exists-batch
    [this indices]
    [this indices conf]
    "Check for each key in a batch if it exists. `indices` is a collection of maps
    of the form `{:index \"key-name\" :set \"set-name\"}`. The result is a
    vector of booleans in the same order as indices.")

  (operate
    [this index set-name expiration operations]
    [this index set-name expiration operations conf]
    "Asynchronously perform multiple read/write operations on a single key in one batch call.
    This method registers the command with an event loop and returns. The event loop thread
    will process the command and send the results to the listener.
    `commands` is a sequence of Aerospike CDT operations.")

  (scan-set [this aero-namespace set-name conf]
    "Scans through the given set and calls a user defined callback for each record that was found.
    Returns a future response that resolves once the scan completes. When the scan completes
    successfully it returns `true`. The scan may be aborted by returning :abort-scan from the callback.
    In that case the return value is `false`.

    The `conf` argument should be a map with the following keys:
      :callback - Function that accepts a com.aerospike.client.Value and an AerospikeRecord.
      :policy -   Optional. com.aerospike.client.policy.ScanPolicy.
      :bins -     Optional. Vector of bin names to return. Returns all bins by default."))

(defprotocol AerospikeAdminOps
  "Admin operations of an Aerospike client."
  (info
    [this node info-commands]
    [this node info-commands conf]
    "Asynchronously make info commands to a node. A node can be retrieved from `get-nodes`.
    `commands` is a seq of strings available from https://www.aerospike.com/docs/reference/info/index.html
    The returned future contains a map from and info command to its response.
    `conf` can contain {:policy InfoPolicy}.")

  (get-nodes [this] "Returns a vector of the client's cluster's nodes.")

  (get-cluster-stats [this]
    "Return a vector of [metric-name metric-val] 2-tuples.
    The metric name is a dot separated string that should be convenient for
    reporting to statsd/graphite. All values are gauges.")

  (healthy? [this] [this operation-timeout-ms]
    "Returns `true` iff the cluster is reachable and can take reads and writes.
    Uses __health-check set to avoid data collisions. `operation-timeout-ms` is
    for total timeout of reads (default is 1s) including 2 retries so a small
    over estimation is advised to avoid false negatives.")

  (stop [this]
    "Gracefully stop a client, waiting until all async operations finish.
    The underlying EventLoops instance is closed iff it was not provided externally."))

(defprotocol ClientEvents
  "Continuation functions that are registered when an async DB operation is called.
  The value returned from those function will be the value of the returned future from the async operation."
  (on-success [this op-name op-result index op-start-time]
    "A continuation function. Registered on the operation future and called when operations succeeds.")
  (on-failure [this op-name op-ex index op-start-time]
    "A continuation function. Registered on the operation future and called when operations fails."))

(defprotocol UserKey
  "Use `create-key` directly to pass a pre-made custom key to the public API.
  When passing a simple String/Integer/Long/ByteArray the key will be created
  automatically for you. If you pass a ready made key, `as-namespace` and
  `set-name` are ignored in API calls."
  (create-key [this as-namespace set-name]))

(defprotocol AerospikeIndex
  (create-key-method ^Key [this ^String as-namesapce ^String set-name]))
