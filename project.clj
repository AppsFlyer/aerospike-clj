(defproject aerospike-clj "0.6.0"
  :description "An Aerospike Clojure client."
  :url "https://github.com/AppsFlyer/aerospike-clj"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[com.aerospike/aerospike-client "4.4.15"]
                 [manifold "0.1.8"]]
  :plugins [[lein-codox "0.10.5"]]
  :codox {:output-path "codox"
          :source-uri "http://github.com/AppsFlyer/aerospike-clj/blob/{version}/{filepath}#L{line}"
          :metadata {:doc/format :markdown}}
  :source-paths ["src/main/clojure"]
  :java-source-paths ["src/main/java"]
  :profiles {:dev {:plugins [[jonase/eastwood "0.3.5"]
                             [lein-cloverage "1.1.1"]]
                   :dependencies [[org.clojure/clojure "1.10.1"]
                                  [criterium "0.4.6"]
                                  [cheshire "5.10.0"]
                                  [com.taoensso/timbre "4.10.0"]
                                  [danlentz/clj-uuid "0.1.9"]
                                  [com.fasterxml.jackson.core/jackson-databind "2.11.1"]]
                   :global-vars {*warn-on-reflection* true}}})
