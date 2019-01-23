## Advanced asynchronous hooks:
Since `aerospike-clj` uses a future based model instead of a callback based model, it is convenient to compose complex asynchronous logic using [manifold](https://github.com/ztellman/manifold).

By implementing `ClientEvents` 2 hooks are exposed that are called for each API call: `on-success` and `on-failure`.

Those hooks are called with valuable information that can be used, for example to configure automatic logging, or telemtry on your client. Here is an example of a such code, that is reporting useful metrics to [statsd](https://github.com/etsy/statsd). So assuming you have some `statsd` namespace tha can connect and report a statsd server, and some `metrics` namespace that is used to properly format the metric names:
```clojure
(ns af-common-rta-aerospike.core
  (:require [aerospike-clj.client :as aero]
            [statsd.metrics :as metrics]
            [statsd.core :as statsd]
            [manifold.deferred :as d]))

(defrecord DBMeter []
  client/ClientEvents
  (on-success [_ op-name op-result _index op-start-time client]
    (statsd/send-timing (metrics/format-statsd-metric (:cluster-name client) op-name "latency")
                        (micros-from op-start-time)
                        STATSD-RATE)
    (statsd/inc-metric (metrics/format-statsd-metric (:cluster-name client) op-name "success"))
    (when (= "read" op-name)
      (if (some? op-result)
         (statsd/inc-metric (metrics/format-statsd-metric (:cluster-name client) "read" "hit"))
         (statsd/inc-metric (metrics/format-statsd-metric (:cluster-name client) "read" "miss"))))
    op-result)
  (on-failure [_ op-name op-ex index op-start-time client]
    (statsd/send-timing (metrics/format-statsd-metric (:cluster-name client) op-name "latency")
                        (micros-from op-start-time)
                        STATSD-RATE)
    (statsd/inc-metric (metrics/format-statsd-metric-fail-aerospike op-ex (:cluster-name client) op-name))
    (d/error-deferred op-ex)))
```
A few notes on the above code:
1. Passed arguments:
  * `op-name`, `op-result` and `index` are strings. They partially used for metrics generation in our case.
  * `client` here is the `IAerospikeClient` instace. You can use its fields here, or you can even `assoc` more keys on it when you create it, to be later used here.
  * `op-start-time` is `(System/nanoTime)`, converted here to microseconds and used to measure latency.
2. The code is using the passed arguments to measure latency, format metrics names. You can easily do other stuff like logging etc.
3. Both `on-success` and `on-failure` return the results passed in. Although this logic is the last logic that happens to the operations results (e.g. after trascoders are being run), the returned result will be what the calling code gets as a returned value.

Finally, hook it to your client:
```clojure
user=> (def c (aero/init-simple-aerospike-client ["localhost"] "test" {:client-events (->DBMeter)}))
```
