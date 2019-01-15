## Implementing `IAerospikeClient`
An exmaple of how to implement, for example a simple sharding client, that talks to several separate clusters:
```clojure
(defrecord ShardedAerospikeClient [^"[Lcom.aerospike.client.AerospikeClient;" acs
                                   ^int shards
                                   ^EventLoop el
                                   ^String dbns
                                   ^String cluster-name
  aero/IAerospikeClient
  (get-client ^AerospikeClient [_ index] (get acs (mod (hash index) shards)))
  (get-client ^AerospikeClient [_] [(first acs)])
  (get-all-clients [_] acs))
```
Then, a factory function creating one would look like:
```clojure
(defn init-sharded-aerospike-client
  [hosts aero-ns conf]
   (let [cluster-name "sharded-99"
         event-loops (:event-loops conf (aero/create-event-loops conf))
         client-policy (aero/create-client-policy event-loops conf)
         clients (into-array AerospikeClient (map #(aero/create-client % client-policy) hosts))]
     (println (format ";; Starting aerospike clients for clusters %s" cluster-name ))
     (map->ShardedAerospikeClient {:acs clients
                                   :shards (count clients)
                                   :el event-loops
                                   :dbns aero-ns
                                   :cluster-name cluster-name})))
```

