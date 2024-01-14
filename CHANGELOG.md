# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

### [4.0.0] - 2024-01-14

#### Changed

* Introduce a `aerospike-clj.batch-client` namespace.
  This namespace extends the `aerospike-clj.client` namespace with batch operations using the `AerospikeBatchOps`
  protocol.
* This will allow clients to use the `aerospike-clj.client` namespace with older versions of the Aerospike Java client
  library, and use the `aerospike-clj.batch-client` namespace with newer versions of the Aerospike Java client library.

### [3.1.0] - 2023-08-22

#### Added

* Add the `aerospike-clj.collections/->list` function, which is similar to `clojure.core/mapv`, but it's more efficient
  when the input is not a Clojure sequence.

#### Changed

* Make the `aerospike-clj.utils/v->array` multi-arity, allowing to pass a `mapper-fn` to map the values before setting
  them into the array.
* Optimize the `aerospike-clj.utils/v->array` function by calling `java.util.Collection#toArray` with a 0-length array,
  this will force the implementation to use the more performant `java.util.Arrays.copyOf`.
* Add contents: write to the Push CI - master action, this should resolve the git push issues from the GitHub actions
  bot.

#### Deprecated

* Deprecate `aerospike-clj.utils/string-keys?`.

## [3.0.0] - 2023-08-03

### Changed

* use aerospike client version 6.1.10
* use test containers for integration tests

### Added

* batch operate support
* completion executor support (with default)
* client-events additional context support

### Removed

* empty test namespace

## [2.0.7] - 2023-08-03

### Changed

* Fixed a bug with reporting metrics for `AerospikeSingleIndexBatchOps/operate`.

### Removed

* 2 unused dev profile plugins.
* Unnecessary integration test.

## [2.0.6] - 2022-12-01

### Changed

* Performance and memory optimization, mainly in the core `aerospike-clj.aerospike-record/record->map` function.

## [2.0.5] - 2022-08-15

### Changed

* TTLs for the mock client are now correctly mocked:
    * TTL is stored (and returned in get operations) as seconds from Aerospike epoch time

### Updated

* Dependencies
    * promesa 6.0.0 -> 8.0.450
    * org.clojure/tools.logging 1.1.0 -> 1.2.4

## [2.0.3] - 2022-07-27

### Changed

* Add missing configuration for eftest to out the test results to target/junit.xml.
* Upgrade `EnricoMi/publish-unit-test-result-action@v1.6` -> `EnricoMi/publish-unit-test-result-action@v1.39`.
* Change the behavior of the mocked `replace-only`.
  It should throw an AerospikeException when the item doesn't exist.

## [2.0.1] - 2021-01-31

### Updated

* Links in README and CI config.

## [2.0.0] - 2021-01-31

### Added

* Aerospike `Key` - can now coerce `java.util.UUID` into keys alongside byte arrays,
  ints, longs, strings and `com.aerospike.client.Value`.
* Created the `protocols` namespace which now holds a myriad of protocols.
    * This includes new protocols that group Aerospike operations by CRUD/admin semantics.
* Can explicitly specify the port in the host string that is passed to the client
  constructor `init-simple-aerospike-client`.
* Integration test namespace now has the `^:integration` metadata:
    * Run unit tests with `lein test`
    * Run integration tests that require a locally-running Aerospike client via `lein test :integration`.

### Changed

* Artifact coordinates in [Clojars](https://clojars.org/) have changed from `aerospike-clj/aerospike-clj`
  to `com.appsflyer/aerospike-clj`.
* Upgraded dependency on [`promesa`](https://github.com/funcool/promesa) from `5.1.0` to `6.0.0`.
* Implementations of `ClientEvents` protocol will no longer get the DB instance
  for runtime parameters. Instead, they should be pre-configured at instance construction time.
* Cleaned up the `client` namespace:
    * Removed the `IAerospikeClient` protocol it can create a collision with `com.aerospike.client.IAerospikeClient`.
      Abstracting over the Java client instance selection is of no concern to a simple
      client that interacts with a single cluster.
        * As a result `SimpleAerospikeClient` now directly uses the vars passed in
          construction time instead of fetching them from the `client` with keywords, e.g. `(:el client)`.
        * The return type of `get-cluster-stats` is no longer a triply-nested vector,
          but a doubly-nested vector.
    * All protocols moved to `protocols` namespace.
    * `SimpleAerospikeClient` record now implements the protocols mentioned above.
* Mock client
    * The `MockClient` record now implements the protocols mentioned above, so
      production code could now have its `SimpleAerospikeClient` swapped with a mock
      client in-place and __without__ using `with-redefs`.
    * Functionality that is needed for unit testing purposes is defined in the
      `Stateful` protocol and `MockClient` instances are extended to this protocol.
* Logging via [`tools.logging`](https://github.com/clojure/tools.logging) as a façade.
* CI
    * No longer runs the lein command `compile` - it would be executed implicitly by `test`

### Removed

* The function `get-multiple` was removed in favor of the protocol method `get-batch`.
* Dependency on [`timbre`](https://github.com/ptaoussanis/timbre).

## [1.0.2] - 2021-01-31

### Added

* This CHANGELOG now follows [keepachangelog](https://keepachangelog.com/en/1.0.0/).
* CI with GitHub Actions.
* Linting with [`clj-kondo`](https://github.com/clj-kondo/clj-kondo).

## [1.0.1] - 2020-09-02

### Changed

* add set to the record returned by batch-read

## [1.0.0] - 2020-08-18

### This is a breaking change.

* Chaining via implementing `ClientEvents` now supports passing also a vector,
  and results in chaining all completion by order they were given.
* The returned result is now a Java(8<) `CompletableFuture` instead of `manifold/Deferred`.
* All listeners factored out of the main `client` namespace.

## [0.6.0] - 2020-07-30

* Support ClientEvents vector to be a vector of completions instead of a single one.
* Bump aerospike lib to 4.4.15

## VERSION 0.5.1

* Added batch-exists
* Bump aerospike lib to 4.4.10

## VERSION 0.5.0

* Added mocking for aerospike client

## VERSION 0.3.8

* Bump aerospike lib to 4.4.9

## VERSION 0.3.7

* Scan support

## VERSION 0.3.6

* Bump aerospike lib to 4.4.6

## VERSION 0.3.5

* Bump aerospike lib to 4.4.4.
* Improve CDT tests (requires testing against aerospike server v4.6)

## VERSION 0.3.4

* Rename set to set-single. liraz.meyer@appsflyer.com

## VERSION 0.3.3

* Support set with `update` policy. liraz.meyer@appsflyer.com

## VERSION 0.3.0

* Support multiple bins! dhruvil.patel@kirasystems.com
* Update java client to 4.4.0.

## VERSION 0.2.7

* Support put with `replace_only` policy. dana.borinski@appsflyer.com

## VERSION 0.2.6

* Update java client to 4.3.1.

## VERSION 0.2.5

* More accurate time measurements. ben.chorin@appsflyer.com

## VERSION 0.2.4

* Bump Java library to 4.3.0.
* Add `put-multiple` API for parallel vectoric put.
* Bug fix: username and password keys in client policy should be strings, not keywords.
* License changed to Apache 2.

[A complete list of all java client related changes](https://www.aerospike.com/download/client/java/notes.html)

[3.1.0]: https://github.com/AppsFlyer/aerospike-clj/pull/69

[3.0.0]: https://github.com/AppsFlyer/aerospike-clj/pull/62

[2.0.7]: https://github.com/AppsFlyer/aerospike-clj/pull/64

[2.0.6]: https://github.com/AppsFlyer/aerospike-clj/releases/tag/2.0.6

[2.0.5]: https://github.com/AppsFlyer/aerospike-clj/compare/2.0.3...2.0.5

[2.0.3]: https://github.com/AppsFlyer/aerospike-clj/releases/tag/2.0.3

[2.0.1]: https://github.com/AppsFlyer/aerospike-clj/compare/2.0.0...2.0.1

[2.0.0]: https://github.com/AppsFlyer/aerospike-clj/compare/1.0.2...2.0.0

[1.0.2]: https://github.com/AppsFlyer/aerospike-clj/compare/1.0.1...1.0.2

[1.0.1]: https://github.com/AppsFlyer/aerospike-clj/compare/1.0.0...1.0.1

[1.0.0]: https://github.com/AppsFlyer/aerospike-clj/compare/0.6.0...1.0.0

[0.6.0]: https://github.com/AppsFlyer/aerospike-clj/compare/0.5.5...0.6.0
