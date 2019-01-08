(defproject aerospike-clj "0.1.0"
  :description "An Aerospike Clojure client."
  :url "https://github.com/AppsFlyer/aerospike-clj"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[com.aerospike/aerospike-client "4.2.3"]
                 [manifold "0.1.8"]
                 [com.taoensso/timbre "4.10.0"]]
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.10.0"]
                                  [criterium "0.4.4"]
                                  [cheshire "5.8.1"]]
                   :global-vars {*warn-on-reflection* true}
                   :plugins [[lein-codox "0.10.5"]]}})
