(defproject aerospike-clj "1.0.2"
  :description "An Aerospike Clojure client."
  :url "https://github.com/AppsFlyer/aerospike-clj"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :source-paths ["src/main/clojure"]
  :java-source-paths ["src/main/java"] ;; currently empty
  :deploy-repositories [["releases" {:url           "https://repo.clojars.org"
                                     :username      :env/clojars_username
                                     :password      :env/clojars_password
                                     :sign-releases false}]
                        ["snapshots" {:url           "https://repo.clojars.org"
                                      :username      :env/clojars_username
                                      :password      :env/clojars_password
                                      :sign-releases false}]]
  :dependencies [[com.aerospike/aerospike-client "4.4.15"]
                 [funcool/promesa "5.1.0"]]
  :profiles {:dev  {:plugins      [[jonase/eastwood "0.3.5"]
                                   [lein-ancient "0.6.15"]
                                   [lein-eftest "0.5.9"]]
                    :dependencies [[org.clojure/clojure "1.10.1"]
                                   [criterium "0.4.6"]
                                   [cheshire "5.10.0"]
                                   [com.taoensso/timbre "4.10.0" :exclusions [org.clojure/tools.reader]]
                                   [danlentz/clj-uuid "0.1.9"]
                                   [com.fasterxml.jackson.core/jackson-databind "2.11.2"]
                                   [clj-kondo "RELEASE"]]
                    :aliases      {"clj-kondo" ["run" "-m" "clj-kondo.main"]
                                   "lint"      ["run" "-m" "clj-kondo.main" "--lint" "src" "test"]}
                    :global-vars  {*warn-on-reflection* true}}
             :ci   {:local-repo "~/.m2/repository"}
             :docs {:plugins [[lein-codox "0.10.7"]]
                    :codox   {:output-path "codox"
                              :source-uri  "http://github.com/AppsFlyer/aerospike-clj/blob/{version}/{filepath}#L{line}"
                              :metadata    {:doc/format :markdown}}}})
