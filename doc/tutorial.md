# Tutorial
## General
### What is this library?
This library is a Clojure wrapper over the [Aerospike Java library](https://www.aerospike.com/docs/client/java/).
The Java Documentation can be found [here](https://www.aerospike.com/apidocs/java/).

For a complete reference of Aerospike Java client internals I warmly recommend
reading the [Java client examples](https://github.com/aerospike/aerospike-client-java/tree/master/examples).

This library wraps the Java code, hides all Clojure Java interop and supplies a
more functional approach to Aerospike. It also adds a few handy features.

A note worth mentioning at the beginning is that the Java client has both
synchronous and asynchronous APIs. This library however, only wraps the asynchronous 
ones. If a synchronous behaviour is still desired, it can be be easily achieved,
as shown below.

Also, although possible to combine several namespaces in a single cluster, this
library uses a single client object per cluster/client.
### Setup
#### Docker
For this tutorial we will take advantage of a locally deployed Aerospike DB with
docker container. The following command should download and install a docker
image with the latest Aerospike image on your machine.
```bash
$ sudo docker run -d --name aerospike -p 3000:3000 -p 3001:3001 -p 3002:3002 -p 3003:3003 aerospike"
```
The DB created contains a single namespace named `test`. On production, Aerospike
is a distributed cluster composed of several nodes.

#### REPL
In order to follow the example further in this tutorial, you should run a Clojure
REPL with the library located in the classpath.
This can be done:
- by cloning the library and running a REPL:
```bash
$ git clone https://github.com/AppsFlyer/aerospike-clj.git
$ cd aerospike-clj
$ lein repl
```
* -or- using [lein-try](https://github.com/avescodes/lein-try):
```bash
$ lein try aerospike-clj "0.2.1"
...
user=> (require '[aerospike-clj.client :as aero])
nil
```
* -or- by referencing it in your project `profiles.clj` and running a repl there:
```clojure
(defproject af-common-rta-aerospike "3.0.0"
  :dependencies [[aerospike-clj "2.0.0"]]
  ; ...
  )
```
## Usage
#### Client creation
First, let's create a client:
```clojure
user=> (require '[aerospike-clj.client :as aero])
nil
user=> (def c (aero/init-simple-aerospike-client ["localhost"] "test"))
;; Starting aerospike clients for clusters localhost with username null
#'user/c
user=>
```
That's it. A client creation requires only a vector of servers to bootstrap from
and a namespace name. Behind the scenes the client constructor is called with a
default [`ClientPolicy`](https://www.aerospike.com/apidocs/java/com/aerospike/client/policy/ClientPolicy.html).
In order to further configure the client for your needs pass a configuration map
as the last argument. The map is a flat map of string keys, corresponding to the
`ClientPolicy` class. To use the Java enumerations, simply uppercase the first character:
```clojure
user=> (def c (aero/init-simple-aerospike-client ["localhost"]
                                                 "test"
                                                 {"failIfNotConnected" true
                                                  "AuthMode" "INTERNAL"
                                                  "username" nil
                                                  "password" nil
                                                  "maxCommandsInProcess" 0}))
;; Starting aerospike clients for clusters localhost with username null
#'user/c
```

Currently, almost all configuration (except default batch/info/query/scan policies) can be passed to
`init-simple-aerospike-client` in the `conf` map. This includes `ClientPolicy` and `EventPolicy`
configurations, flattened together (e.g `maxCommandsInProcess`) :

The Java client can cache all API related policies for you, and then uses them when a
`nil` policy is passed to the API call. This is worthwhile because most of the time,
the same policies are used throughout the application lifetime. Those caches are
initiated with the (mostly sane) defaults. You can change them by creating custom ones:
```clojure
user=> (require '[aerospike-clj.policy :as policy])
nil
user=> (def new-wp (policy/map->write-policy {"CommitLevel" "COMMIT_MASTER" "sendKey" true}))
#'user/new-wp
user=> (.commitLevel new-wp)
Reflection warning...
#object[com.aerospike.client.policy.CommitLevel 0x2f1f3fef "COMMIT_MASTER"]
user=> (.sendKey new-wp)
Reflection warning...
true
```
##### Important note:
The functions `map->policy` and `map->write-policy` are slow due to reflection and
are here to save you some Java interop. Since they are slow, **do not** use them
to create one-time-use policies for each API call. Use them just to tweak the defaults:
```clojure
user=> (def c (aero/init-simple-aerospike-client ["localhost"] "test" {"writePolicyDefault" new-wp}))
#'user/c
user=> (.commitLevel (.writePolicyDefault (aero/get-client c)))
#object[com.aerospike.client.policy.CommitLevel 0x2f1f3fef "COMMIT_MASTER"]
```

Those `AerospikeClient` fields are `final` hence can only be set on client creation,
but you can also have a few useful policies `def`ed somewhere and pass them later
under the `:policy` key of the API calls.

#### Querying
Querying is only possible (read: easy) via the asynchronous APIs of the client.
`aerospike-clj` also converts the callback model of the underlying APIs to a future
based model (using [`promesa`](https://github.com/funcool/promesa)). This allows
users to configure additional logic to happen when the response returns.
But first, let's see a simple query:
```clojure
user=> (require '[aerospike-clj.protocols :as pt])
nil
user=> (def f (pt/get-single c "not-there" "set-name"))
user=> (type f)
java.util.concurrent.CompletableFuture
user=> (deref f)
nil
```
Joy! We got a future answer. In this case, the deferred result is `nil`, since the
key `"not-there"` is missing. Futures can be composed with logic that happens once
they are delivered (for a complete documentation, see `promesa`'s docs [here](https://funcool.github.io/promesa/latest/). 
Behold:
```clojure
user=> (require '[promesa.core :as p])
nil
user=> (p/then (pt/get-single c "index" "set-name")
  #_=>         #(if %1 (prn "good!") (prn "not there")))
"not there"
```
If the record existed we would get an `AerospikeRecord`:
```clojure
user=> (pt/put c "index" "set-name" 42 1000)
#object[java.util.concurrent.CompletableFuture 0x16283d4b "pending"]
user=> (def f (pt/get-single c "index" "set-name"))
#'user/f
user=> (deref f)
#aerospike_clj.client.AerospikeRecord{:payload 42, :gen 1, :ttl 284805805}
user=>
```
We observe a few new things:
1. The `put` API also returns futures. Those will contain the operation's result
when it completes.
2. The `put` API requires an additional argument, the TTL. Although Aerospike
cluster namespaces have a default TTL, and there is also a default TTL in the
Java client, we chose to make your life harder here. This is because we believe
that TTLs are too important to be default, and programmers must think about them.
3. In the query result we got a Clojure record with a triplet: `:payload` with
the value, `:gen` for the record generation, and a `:ttl` for the record TTL, in
Aerospike format. This resembles the [Record class](https://www.aerospike.com/apidocs/java/com/aerospike/client/Record.html).
In order to convert the TTL to Unix epoch you can call `expiry-unix` on it (see below).

#### Unix epoch TTL
Aerospike returns a TTL on the queried records that is Epoch style, but with a
different "beginning of time" which is "2010-01-01T00:00:00Z". Call `expiry-unix`
with the returned TTL to get a TTL reative to UNIX epoch if you want to convert
it later to a more standard timestamp.

```clojure
user=> (-> f
  #_=>     deref
  #_=>     :ttl
  #_=>     aero/expiry-unix
  #_=>     java.time.Instant/ofEpochSecond
  #_=>     str)
"2019-01-10T08:43:25Z"
```
Let's do it in an asynchronous manner:
```clojure
user=> (p/chain (pt/get-single c "index" "set-name")
  #_=>          :ttl
  #_=>          aero/expiry-unix
  #_=>          #(java.time.Instant/ofEpochSecond %)
  #_=>          str
  #_=>          println)
#object[java.util.concurrent.CompletableFuture 0x4a9620a9 "pending"]
2019-01-10T09:02:45Z
```
We got a deferred back and some time later the whole chain of composed logic
was triggered. We can also get the result once multiple required records return.
We will simply get a sequence of `AerospikeRecord`s once all of them arrive:
```clojure
user=> (run! #(pt/put c (str %1) "set-name" %1 1000) (range 5))
nil
user=> @(p/then (aero/get-multiple c (map str (range 5)) (repeat "set-name"))
  #_=>          #(map :payload %))
(0 1 2 3 4)
```

##### Sync Querying
Since the returned future objects can be easily `deref`ed, simply adding a `@`
before queries makes them synchronous.

#### Using Transcoders
The library takes advantage of futures' ability to compose and allows you to configure
a `:transcoder` to conveniently set this logic:
* `get` Transcoders are functions of the **AerospikeRecord instance**, not the
`deferred` value of it.
* `put` Transcoders are functions on the passed **payload**. They are called _before_
the request is even put on the event-loop.

##### On get:
```clojure
user=> (pt/put c "index" "set-name" 42 1000)
#object[java.util.concurrent.CompletableFuture 0x4a9620a9 "pending"]
user=> (defn inc-transcoder [rec] (when rec
  #_=>                                  (update rec :payload inc)))
#'user/inc-transcoder
user=> (p/chain (pt/get-single c "index" "set-name" {:transcoder inc-transcoder})
  #_=>          :payload
  #_=>          println)
#object[java.util.concurrent.CompletableFuture 0x4a9620af "pending"]
43
```

##### On put:

The transcoder here is a function on the _payload_ itself
```clojure
user=> (pt/put c "17" "set-name" 1 1000 {:transcoder str})
#object[java.util.concurrent.CompletableFuture 0x4d025d9b "pending"]
user=> @(pt/get-single c "17" "set-name" {:transcoder #(:payload %1)})
"1"
```
The transcoder option saves some boilerplate and can be easily used to do more
useful stuff, like de-serializing or (de)compressing data on the client side.
