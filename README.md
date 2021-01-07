# Aerospike-clj

An opinionated Clojure library wrapping Aerospike Java Client.

[![Clojars Project](https://img.shields.io/clojars/v/aerospike-clj.svg)](https://clojars.org/aerospike-clj)

[![Build Status](https://travis-ci.com/AppsFlyer/aerospike-clj.svg?branch=master)](https://travis-ci.com/AppsFlyer/aerospike-clj)

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
- Converts Java client's callback model into Java(8) `CompletableFuture` based API.
- Expose passing functional (asynchronous) transcoders over payloads (both put/get).
- Health-check utility.
- Functions return Clojure records.

# Maturity:
- Feature completeness: ~~mostly~~ near complete.
- Stability: production ready. Actively and widely used in production.

# Opinionated:
- Non blocking only: Expose only the non-blocking API. Block with `deref` if you like.
- Futures instead of callbacks. Futures (and functional chaining) are more composable and less cluttered.
If synchronous behaviour is still desired, the calling code can still `deref` (`@`) the returned future object.
For a more sophisticated coordination, a variety of control mechanisms can be used by directly using Java's
`CompletableFuture` API or the more Clojure friendly [promesa](https://github.com/funcool/promesa) (which is also used internally),
or via the library using [transcoders](https://appsflyer.github.io/aerospike-clj/index.html) or
[hooks](https://appsflyer.github.io/aerospike-clj/advanced-async-hooks.html).
- Follows the method names of the underlying Java APIs.
- TTLs should be explicit, and developers should think about them. Forces passing a TTL and not use the cluster default
(This can be still achieved by passing the [special values](https://www.aerospike.com/apidocs/java/com/aerospike/client/policy/WritePolicy.html#expiration) -2,-1 or 0).
- Minimal dependencies.
- Single client per Aerospike namespace. Namespaces in Aerospike usually indicate different cluster configurations.
In order to reduce overhead for clusters with more than a single namespace create 2 client instances and share an event
loop between them.

# Limitations/caveats

# TBD
- Support Java 11

## Usage:
#### Most of the time just create a simple client (single cluster)
```clojure
user=> (require '[aerospike-clj.client :as aero])
nil
user=> (def c (aero/init-simple-aerospike-client
  #_=>          ["aerospike-001.com", "aerospik-002.com"] "my-ns" {:enable-logging true}))
```

It is possible to inject additional asynchronous user-defined behaviour. To do that add an implementation of  the
`ClientEvents` protocol.
Some useful info is passed in-order to support metering and to read client configuration. `op-start-time` is
`(System/nanoTime)`, see more [here](https://appsflyer.github.io/aerospike-clj/advanced-async-hooks.html).

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
; for better performance, a `deftype` might be preferred over `reify`, if possible.
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
user=> (require '[promesa.core :as p])
nil
user=> (aero/put c "index" "set-name" 42 1000)
#object[java.util.concurrent.CompletableFuture 0x6264b083 "pending"]
user=> (def f (aero/get-single c "index" "set-name"))
#'user/f
user=> (p/chain (aero/get-single c "index" "set-name")
  #_=>          :ttl
  #_=>          aero/expiry-unix
  #_=>          #(java.time.Instant/ofEpochSecond %)
  #_=>          str
  #_=>          println)
2020-08-13T09:52:49Z
#object[java.util.concurrent.CompletableFuture 0x654830f5 "pending"]
```
We actually get back a record with the payload, the DB generation and the TTL (in an Aerospike style EPOCH format).
```clojure
user=> @(aero/get-single c "index" "set-name")
#aerospike_clj.client.AerospikeRecord{:payload 42, :gen 1, :ttl 285167713}
```

#### Unix EPOCH TTL
Aerospike returns a TTL on the queried records that is epoch style, but with a different "beginning of time" which is "2010-01-01T00:00:00Z".
Call `expiry-unix` with the returned TTL to get a TTL relative to the UNIX epoch.

## Testing
Testing is performed against a local Aerospike instance. You can also run an instance inside a docker container:

```shell
$ sudo docker run -d --name aerospike -p 3000:3000 -p 3001:3001 -p 3002:3002 -p 3003:3003 aerospike
$ lein test
```

#### Mocking in application unit tests
For unit tests purposes you can use a mock client that implements the client protocol.  

Usage:

```clojure
(ns com-example.app 
  (:require [clojure.test :refer [deftest use-fixtures]]
            [aerospike-clj.mock-client :refer [init-mock]]))

(use-fixtures :each init-mock)

(deftest ...) ;; define your application unit tests as usual
```

The sample code executes on every test run. It initializes the mock and runs
the test within a `with-redefs` - rebinding all the calls to functions
in `aerospike-clj.client` to the mock.

Note: If the production client is initiated using a state management framework,
you would also need to stop and restart the state on each test run.


## Contributing
PRs are welcome!

## License

Distributed under the Apache 2.0 License - found [here](https://github.com/AppsFlyer/aerospike-clj/blob/master/LICENSE).
