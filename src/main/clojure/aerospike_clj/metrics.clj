(ns aerospike-clj.metrics
  (:require [clojure.string :as s])
  (:import [com.aerospike.client.cluster ClusterStats NodeStats Node]
           [com.aerospike.client.async EventLoopStats]))

(defn node-ip [^Node node]
  (s/replace (.. node (getAddress) (getAddress) (getHostAddress)) "." "-"))

(defn construct-node-stats [^NodeStats raw-node-stats]
  {:node (node-ip (.node raw-node-stats))
   :async {:in-pool (.inPool (.async raw-node-stats))
           :in-use (.inUse (.async raw-node-stats))}
   :sync {:in-pool (.inPool (.sync raw-node-stats))
          :in-use (.inUse (.sync raw-node-stats))}})

(defn construct-event-loop-stats [^EventLoopStats event-loops-stats]
  {:in-process (.processSize event-loops-stats)
   :in-queue (.queueSize event-loops-stats)})

(defn construct-cluster-metrics [^ClusterStats raw-cluster-metrics]
  {:threads-in-use (.threadsInUse raw-cluster-metrics)
   :nodes (mapv construct-node-stats (.nodes raw-cluster-metrics))
   :event-loops (mapv construct-event-loop-stats (.eventLoops raw-cluster-metrics))})

(defn cluster-metrics->dotted [cluster-metrics]
  (->> (for [node (:nodes cluster-metrics)
             conn-type [:sync :async]
             metric [:in-pool :in-use]]
         [(s/join "." ["nodes" (:node node) (name conn-type) (name metric)])
          (get-in node [conn-type metric])])
       (concat [["threads-in-use" (:threads-in-use cluster-metrics)]])
       (concat
         (for [[i el] (map-indexed vector (:event-loops cluster-metrics))
               stage [:in-process :in-queue]]
           [(s/join "." ["event-loops" i (name stage)]) (stage el)]))
       vec))

