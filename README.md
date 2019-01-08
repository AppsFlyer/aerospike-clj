# Aerospike-clj

An opinionated Clojure library wrapping Aerospike Java Client.

![](https://clojars.org/aerospike-clj/latest-version.svg)

# Requirements:
- Java 8
- Clojure 1.8

# Features:
- Converts Java client's callback model into a future (manifold/deferred) based API.
- Expose passing functional transdcoders over payloads (both put/get).
- Health-check utility.
- Functions return Clojure maps.

# Opinionated:
- Non blocking only: Expose only the non-blocking API. Block with `deref` if you like.
- Futures instead of callbacks. Futures (and functional chaining) are more composable and less cluttered.
If a synchronous behaviour is still desired, the calling code can still deref (`@`) the returned future object. For a more sophisticated coordination, a variety of control mechanism is supplied by [manifold/deferred](https://github.com/ztellman/manifold/blob/master/docs/deferred.md).
- Tries to follow the method names of the underlying Java API (with Clojure standard library limitations)
- TTLs should be explicit, and developers should think about them. Forces passing a ttl and not use the cluster default.
- Minimal dependencies.
- Single client per Aerospike namespace.

# Limitations/ caveats
- Currently supports only single bin records.
- Does not expose batch operations.

# TBD
- use batch asynchronous APIs

## Usage:
#### Most of the time just create a simple client (single cluster)
```clojure
(require '[aerospike-clj.client :as client)

(def db (client/init-simple-aerospike-client
          ["aerospike-001.com", "aerospik-002.com"] "my-ns" {:enable-logging true}))
```

#### It is possible to inject additional asynchronous user-defined behaviour. To do that add an instance of ClientEvents. Some useful info is passed in in-order to support metering and to read client configuration. `op-start-time` is `(System/nanoTime)`.

```clojure
(let [c (client/init-simple-aerospike-client
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
    (is (true? @(client/create db "a-key" "a-set" data 100)))
    (let [{:keys [payload gen]} @(client/get-single-with-meta db "a-key" "a-set")]
      (is (= data payload))
      (is (= 1 gen))))
```

#### Unix EPOCH TTL
Aerospike returns a TTL on the queried records that is Epoch style, but with a different "beginning of time" which is "2010-01-01T00:00:00Z". Call `expiry-unix` with the returned TTL to get a UNIX TTL if you want to convert it later to a more standard timestamp.

### Put
```clojure
(let [data (rand-int 1000)]
    (is (true? @(client/put db "another-key" "a-set" data 100{:record-exist-action :replace :transcoder identity})))
    (let [{:keys [payload gen]} @(client/get-single-with-meta db "another-key" "a-set")]
      (is (= data payload))
      (is (= 1 gen))))
```

## License

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
