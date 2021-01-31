## Advanced asynchronous hooks:
Since `aerospike-clj` uses a future based model instead of a callback based model,
it is convenient to compose complex asynchronous logic using [promesa](https://github.com/funcool/promesa).

By implementing the `ClientEvents` protocol 2 hooks are exposed that are called
for each API call: `on-success` and `on-failure`.

Those hooks are called with valuable information that can be used, for example
to configure automatic logging or metrics on your client. Here is an example of
such code that is reporting useful metrics to [statsd](https://github.com/etsy/statsd).
So assuming you have some `statsd` namespace tha can connect and report to a `statsd`
server, and some `metrics` namespace that is used to properly format the metric names:
```clojure
(ns af-common-rta-aerospike.core
  (:require [aerospike-clj.protocols :as pt]
            [statsd.metrics :as metrics]
            [statsd.core :as statsd]
            [promesa.core :as p]))

(defrecord DBMeter [cluster-name]
  pt/ClientEvents
  (on-success [_ op-name op-result _index op-start-time]
    (statsd/send-timing (metrics/format-statsd-metric cluster-name op-name "latency")
                        (micros-from op-start-time)
                        STATSD-RATE)
    (statsd/inc-metric (metrics/format-statsd-metric cluster-name op-name "success"))
    (when (= "read" op-name)
      (if (some? op-result)
         (statsd/inc-metric (metrics/format-statsd-metric cluster-name "read" "hit"))
         (statsd/inc-metric (metrics/format-statsd-metric cluster-name "read" "miss"))))
    op-result)
  (on-failure [_ op-name op-ex index op-start-time]
    (statsd/send-timing (metrics/format-statsd-metric cluster-name op-name "latency")
                        (micros-from op-start-time)
                        STATSD-RATE)
    (statsd/inc-metric (metrics/format-statsd-metric-fail-aerospike op-ex (:cluster-name client) op-name))
    (p/rejected! op-ex)))
```
A few notes on the above code:
1. Passed arguments:
  * `op-name`, `op-result` and `index` are strings. They are partially used for
  metrics generation in our case.
  * `op-start-time` is `(System/nanoTime)`, converted here to microseconds and
  used to measure latency.
2. The code is using the passed arguments to measure latency and format metrics'
names. You can easily do other stuff like logging, etc.
3. Both `on-success` and `on-failure` return the results passed in. Although this
logic is the last logic that happens to the operations' results (e.g. after
transcoders are called), the returned result will be what the calling code gets
as a returned value.

Finally, hook it to your client:
```clojure
user=> (def c (aero/init-simple-aerospike-client ["localhost"] "test" {:client-events (->DBMeter "test-cluster")}))
```
