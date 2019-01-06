# Aerospike-clj

An opinionated Clojure library wrapping Aerospike Java Client.

# Requirements:
- Java 8
- Clojure 1.8

# Features:
- Converts Java client's callback model into a future based API.
- Expose passing functional transducers over payloads (both put/get).
- Health-check utility.
- Functions return Clojure maps.
- Additional wrappers for GZip, JSON etc.

# Opinionated:
- Non blocking only: Expose only the non-blocking API. Block with `deref` if you like.
- Futures instead of callbacks. Futures (and functional chaining) are more composable and less cluttered.
By returning manifold/deffered clients can still deref (@) every DB query for a sync behavior. For a more sophisticated coordination the client code can use the variety of control mechanism supplied by [manifold/deferred](https://github.com/ztellman/manifold/blob/master/docs/deferred.md).
- Tries to follow the method names of the underlying Java API (with Clojure standard library limitations)
- TTLs should be explicit, and developers should think about them. Forces passing a ttl and not use the cluster default.
- Minimal dependencies.

# Limitations/ caveats
- Currently supports only single bin records.
- Does not expose batch operations.

# TBD
- use batch asynchronous APIs

## Usage:
#### Most of the time just create a simple client (single cluster)
```clojure
(def db (init-simple-aerospike-client
          ["aerospike-001.com", "aerospik-002.com"] "my-ns" {:enable-logging true}))
```

#### It is possible to inject additional asynchronous user-defined behaviour. To do that add an instance of ClientEvents. Some useful info is passed in in-order to support metering and to read client configuration. `op-start-time` is `(System/nanoTime)`.

```clojure
(let [c (core/init-simple-aerospike-client
          ["localhost"]
          "test"
          {:client-events (reify ClientEvents
                            (on-success [_ op-name op-result index op-start-time db]
                              (when (:enable-logging? db)
                                (println op-name "success!")))
                            (on-failure [_  op-name op-ex index op-start-time db]
                              (println "oh-no" op-name "failed on index" index)))})]

  (get-single-with-meta c "index" "set-name"))
```

### Query
```clojure
(let [data (rand-int 1000)]
    (is (true? @(core/create db "a-key" "a-set" data 100)))
    (let [{:keys [payload gen]} @(core/get-single-with-meta db "a-key" "a-set")]
      (is (= data payload))
      (is (= 1 gen))))
```


## License

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
