#### VERSION 2.0.0
#### Breaking changes
* Implementations of `ClientEvents` protocol will no longer get the DB instance 
for runtime parameters. Instead, they should be pre-configured at instance construction time.
* Created the `protocols` namespace which now holds a myriad of protocols.
  * This includes new protocols that group Aerospike operations by CRUD/admin semantics.
* Cleaned up the `client` namespace:
  * Removed the `IAerospikeClient` protocol it can create a collision with `com.aerospike.client.IAerospikeClient`. 
  Abstracting over the Java client instance selection is of no concern to a simple 
  client that interacts with a single cluster.
    * As a result `SimpleAerospikeClient` now directly uses the vars passed in 
    construction time instead of fetching them from the `client` with keywords, e.g. `(:el client)`.
    * The return type of `get-cluster-stats` is no longer a triply-nested vector 
    but a doubly-nested vector.
  * All protocols moved to `protocols` namespace.
  * `SimpleAerospikeClient` record now implements the protocols mentioned above.
* Mock client
  * The `MockClient` record now implements the protocols mentioned above, so 
  production code could now have its `SimpleAerospikeClient` swapped with a mock 
  client in-place and __without__ using `with-redefs`.
  * Functionality that is needed for unit testing purposes is defined in the 
  `Stateful` protocol and `MockClient` instances are extended to this protocol.
  * The mock client namespace moved to the `test` directory.
* Integration test namespace now has the `^:integration` metadata:
  * Run unit tests with `lein test`
  * Run integration tests that require a locally-running Aerospike client via `lein test :integration`.
* CI
  * No longer runs the lein command `compile` - it would be executed implicitly by `test`
* Aerospike `Key` - can now coerce `java.util.UUID` into keys alongside byte arrays, 
 ints, longs, strings and `com.aerospike.client.Value`.

#### Non-breaking changes
* CI
  * No longer runs the lein command `compile` - it would be executed implicitly by `test`
* Integration test namespace now has the `^:integration` metadata:
  * Run unit tests with `lein test`
  * Run integration tests that require a locally-running Aerospike client via `lein test :integration`.

#### VERSION 1.0.0
#### This is a breaking change.
* Chaining via implementing `ClientEvents` now supports passing also a vector,
  and results in chaining all completion by order they were given.
* The returned result is now a Java(8<) `CompletableFuture` instead of `manifold/Deferred`.
* All listeners factored out of the main `client` namespace.

#### VERSION 0.6.0
* Support ClientEvents vector to be a vector of completions instead of a single one.
* Bump aerospike lib to 4.4.15

#### VERSION 0.5.1
* Added batch-exsits
* Bump aerospike lib to 4.4.10

#### VERSION 0.5.0
* Added mocking for aerospike client

#### VERSION 0.3.8
* Bump aerospike lib to 4.4.9

#### VERSION 0.3.7
* Scan support

#### VERSION 0.3.6
* Bump aerospike lib to 4.4.6

#### VERSION 0.3.5
* Bump aerospike lib to 4.4.4.
* Improve CDT tests (requires testing against aerospike server v4.6)

#### VERSION 0.3.4
* Rename set to set-single. liraz.meyer@appsflyer.com

#### VERSION 0.3.3
* Support set with `update` policy. liraz.meyer@appsflyer.com

#### VERSION 0.3.0
* Support multiple bins! dhruvil.patel@kirasystems.com
* Update java client to 4.4.0.

#### VERSION 0.2.7
* Support put with `replace_only` policy. dana.borinski@appsflyer.com

#### VERSION 0.2.6
* Update java client to 4.3.1.

#### VERSION 0.2.5
* More accurate time measurements. ben.chorin@appsflyer.com

#### VERSION 0.2.4

* Bump Java library to 4.3.0.
* Add `put-multiple` API for parallel vectoric put.
* Bug fix: username and password keys in client policy should be strings, not keywords.
* License changed to Apache 2.


[A complete list of all java client related changes](https://www.aerospike.com/download/client/java/notes.html)
