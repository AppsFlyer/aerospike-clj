# Aerospike-clj

An opinionated Clojure library wrapping Aerospike Java Client.

[![Clojars Project](https://img.shields.io/clojars/v/com.appsflyer/aerospike-clj.svg)](https://clojars.org/com.appsflyer/aerospike-clj)

[![Build Status](https://img.shields.io/github/workflow/status/AppsFlyer/aerospike-clj/Push%20CI%20-%20master?event=push&branch=master&label=build%20%26%20test)](https://github.com/AppsFlyer/aerospike-clj/actions)
# Docs:
[Generated docs](https://appsflyer.github.io/aerospike-clj/)

## Tutorial
[here.](https://appsflyer.github.io/aerospike-clj/tutorial.html)
## More advanced docs
* [Advanced asynchronous hooks.](https://appsflyer.github.io/aerospike-clj/advanced-async-hooks.html)

# Requirements
- Java 8
- Clojure 1.8
- Aerospike server version >= `4.9.0`
- Clojure version >= `1.11.0`

# Features
- Converts Java client's callback model into Java(8) `CompletableFuture` based API.
- Expose passing functional (asynchronous) transcoders over payloads (both put/get).
- Health-check utility.
- Functions return Clojure records.

# Maturity
- Feature completeness: ~~mostly~~ near complete.
- Stability: production ready. Actively and widely used in production.

# Opinionated
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

## Usage

```clojure
user=> (require '[aerospike-clj.client :as aero])
nil
user=> (def c (aero/init-simple-aerospike-client
  #_=>          ["aerospike-001.com", "aerospik-002.com"] "my-ns" {:enable-logging true}))
```

It is possible to inject additional asynchronous user-defined behaviour. To do that add an implementation of  the
`ClientEvents` protocol during client initialization or per operation.  
Some useful info is passed in-order to support metering and to read client configuration. `op-start-time` is
`(System/nanoTime)`. 
see more [here](https://appsflyer.github.io/aerospike-clj/advanced-async-hooks.html).

```clojure
(let [c (aero/init-simple-aerospike-client
          ["localhost"]
          "test"
          {:client-events (reify ClientEvents
                            (on-success [_ op-name op-result index op-start-time ctx]
                              (when (-> ctx :enable-logging?)
                                (println op-name "success!")))
                            (on-failure [_  op-name op-ex index op-start-time ctx]
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
### Unit tests
Executed via running `lein test`.

### Integration tests
Testing is performed against a local Aerospike docker container.

#### Mocking in application unit tests
For unit tests purposes you can use a mock client that implements the client protocols: `MockClient`.

Usage:

```clojure
(ns com-example.app 
  (:require [clojure.test :refer [deftest use-fixtures]]
            [aerospike-clj.protocols :as pt]
            [aerospike-clj.mock-client :as mock])
  (:import [aerospike_clj.client SimpleAerospikeClient]))

(def ^:dynamic ^SimpleAerospikeClient client nil)

(defn- bind-client-to-mock [test-fn]
  (binding [client (mock/create-instance)]
    (test-fn)))

(use-fixtures :each bind-client-to-mock)

(deftest ...) ;; define your application unit tests as usual
```

The sample code executes on every test run. It initializes the mock with a proper type hint
so you can just invoke all client protocol methods on it.

Note: If the production client is initiated using a state management framework,
you would also need to stop and restart the state on each test run.


## Contributing
PRs are welcome with these rules:
* A PR should increment the project's version in [`project.clj`](project.clj) according
to Semantic Versioning.
* A PR should have its above version set to `SNAPSHOT`, e.g. `1.0.2-SNAPSHOT`.
Once it will be merged into `master` this suffix would be trimmed before release.
* All PRs would be linted and tested. Passing lint and tests is a reuirement for
maintainers to review the PR.

## License

Distributed under the Apache 2.0 License - found [here](https://github.com/AppsFlyer/aerospike-clj/blob/master/LICENSE).
