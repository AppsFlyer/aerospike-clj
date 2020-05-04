(defproject aerospike-clj "0.5.2"
  :description "An Aerospike Clojure client."
  :url "https://github.com/AppsFlyer/aerospike-clj"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[com.aerospike/aerospike-client "4.4.10"]
                 [manifold "0.1.8"]
                 [com.taoensso/timbre "4.10.0"]]
  :plugins [[lein-codox "0.10.5"]]
  :codox {:output-path "codox"
          :source-uri "http://github.com/AppsFlyer/aerospike-clj/blob/{version}/{filepath}#L{line}"
          :metadata {:doc/format :markdown}}
  :profiles {:dev {:plugins [[jonase/eastwood "0.3.5"]
                             [lein-cloverage "1.1.1"]]
                   :dependencies [[org.clojure/clojure "1.10.1"]
                                  [criterium "0.4.5"]
                                  [cheshire "5.10.0"]
                                  [com.taoensso/timbre "4.10.0"]
                                  [danlentz/clj-uuid "0.1.9"]
                                  [com.fasterxml.jackson.core/jackson-databind "2.10.2"]]
                   :global-vars {*warn-on-reflection* true}}})
