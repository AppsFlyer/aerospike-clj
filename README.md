# Aerospike-clj

An opinionated Clojure library wrapping Aerospike Java Client.

[![Clojars Project](https://img.shields.io/clojars/v/aerospike-clj.svg)](https://clojars.org/aerospike-clj)

[![Build Status](https://travis-ci.com/AppsFlyer/aerospike-clj.svg?branch=master)](https://travis-ci.com/AppsFlyer/aerospike-clj)

[![Coverage Status](https://coveralls.io/repos/github/AppsFlyer/aerospike-clj/badge.svg?branch=master)](https://coveralls.io/github/AppsFlyer/aerospike-clj?branch=master)

# Docs:
[Generated docs](https://appsflyer.github.io/aerospike-clj/)
## Tutorial:
[here.](https://appsflyer.github.io/aerospike-clj/tutorial.html)
## More advanced docs:
* [Advanced asynchronous hooks.](https://appsflyer.github.io/aerospike-clj/advanced-async-hooks.html)
* [Implementing your own client.](https://appsflyer.github.io/aerospike-clj/implementing-clients.html)

# Requirements:
- Java 8
- Clojure 1.8

# Features:
- Converts Java client's callback model into a future (manifold/deferred) based API.
- Expose passing functional transcoders over payloads (both put/get).
- Health-check utility.
- Functions return Clojure records.

# Maturity:
- Feature completeness: mostly.
- Stability: production ready. We actively use this library in production.

# Opinionated:
- Non blocking only: Expose only the non-blocking API. Block with `deref` if you like.
- Futures instead of callbacks. Futures (and functional chaining) are more composable and less cluttered.
If a synchronous behaviour is still desired, the calling code can still deref (`@`) the returned future object. For a more sophisticated coordination, a variety of control mechanisms are supplied by [manifold/deferred](https://github.com/ztellman/manifold/blob/master/docs/deferred.md), or via the library using [transcoders](https://appsflyer.github.io/aerospike-clj/index.html) or [hooks](https://appsflyer.github.io/aerospike-clj/advanced-async-hooks.html).
- Follows the method names of the underlying Java APIs.
- TTLs should be explicit, and developers should think about them. Forces passing a ttl and not use the cluster default.
- Minimal dependencies.
- Single client per Aerospike namespace. Namespaces in Aerospike usually indicate different cluster configurations. In order to reduce overhead for clusters with more than a single namespace create 2 client objects and share an event loop between them.

# Limitations/ caveats
- Currently supports only single bin records.
- Does not expose batch/scan operations. Batch writes are supported via `put-multiple`.

# TBD
- ~~Support batch asynchronous APIs.~~
- Support batch put asynchronous API.

## Usage:
#### Most of the time just create a simple client (single cluster)
```clojure
user=> (require '[aerospike-clj.client :as aero])
nil
user=> (def c (aero/init-simple-aerospike-client
  #_=>          ["aerospike-001.com", "aerospik-002.com"] "my-ns" {:enable-logging true}))
```

It is possible to inject additional asynchronous user-defined behaviour. To do that add an instance of `ClientEvents`. Some useful info is passed in in-order to support metering and to read client configuration. `op-start-time` is `(System/nanoTime)` [more here](https://appsflyer.github.io/aerospike-clj/advanced-async-hooks.html).

```clojure
(let [c (aero/init-simple-aerospike-client
          ["localhost"]
          "test"
          {:client-events (reify ClientEvents
                            (on-success [_ op-name op-result index op-start-time db]
                              (when (:enable-logging? db)
                                (println op-name "success!")))
                            (on-failure [_  op-name op-ex index op-start-time db]
                              (println "oh-no" op-name "failed on index" index)))})]

  (get-single c "index" "set-name"))
```

### Query/Put
For demo purposes we will use a docker based local DB:
```shell
$ sudo docker run -d --name aerospike -p 3000:3000 -p 3001:3001 -p 3002:3002 -p 3003:3003 aerospike
```
And connect to it:
```clojure
user=> (def c (aero/init-simple-aerospike-client ["localhost"] "test"))
#'user/db
```

```clojure
user=> (require '[manifold.deferred :as d])
nil
user=> (aero/put c "index" "set-name" 42 1000)
<< … >>
user=> (def f (aero/get-single c "index" "set-name"))
#'user/f
user=> (d/chain (aero/get-single c "index" "set-name")
  #_=>          :ttl
  #_=>          aero/expiry-unix
  #_=>          #(java.time.Instant/ofEpochSecond %)
  #_=>          str
  #_=>          println)
<< … >>
2019-01-10T09:02:45Z
```
We actually get back a record with the payload, the DB generation and the ttl (in an Aerospike style EPOCH format).
```clojure
user=> @(aero/get-single c "index" "set-name")
#aerospike_clj.client.AerospikeRecord{:payload 42, :gen 1, :ttl 285167713}
```

#### Unix EPOCH TTL
Aerospike returns a TTL on the queried records that is Epoch style, but with a different "beginning of time" which is "2010-01-01T00:00:00Z". Call `expiry-unix` with the returned TTL to get a UNIX TTL if you want to convert it later to a more standard timestamp.

## Testing
Testing is performed against a local Aerospike running in the latest docker

```shell
$ sudo docker run -d --name aerospike -p 3000:3000 -p 3001:3001 -p 3002:3002 -p 3003:3003 aerospike
$ lein test
```

## Contributing
PRs are welcome!

## License

Distributed under the Apache 2.0 License - found [here](https://github.com/AppsFlyer/aerospike-clj/blob/master/LICENSE).
