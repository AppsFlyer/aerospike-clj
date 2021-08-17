## This library follows [Semantic Versioning](https://semver.org).
## This CHANGELOG follows [keepachangelog](https://keepachangelog.com/en/1.0.0/).

### VERSION 1.0.3
#### Updated:
* Bumped `com.aerospike/aerospike-client` to `4.4.18`.
* Bumped `funcool/promesa` to `6.0.2`.

#### Changed:
* Using a different method from `com.aerospike.client.Host` to parse a hosts strings
  that contains TLS names for seed nodes in order to allow connecting to a TLS-enabled
  cluster.

### VERSION 1.0.2
#### Added:
* This CHANGELOG now follows [keepachangelog](https://keepachangelog.com/en/1.0.0/).
* CI with GitHub Actions.
* Linting with [`clj-kondo`](https://github.com/clj-kondo/clj-kondo).

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
