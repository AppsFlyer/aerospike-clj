(defproject com.appsflyer/aerospike-clj "2.0.7-SNAPSHOT"
  :description "An Aerospike Clojure client."
  :url "https://github.com/AppsFlyer/aerospike-clj"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :java-source-paths ["src/main/java"]
  :source-paths ["src/main/clojure"]
  :deploy-repositories [["releases" {:url           "https://repo.clojars.org"
                                     :username      :env/clojars_username
                                     :password      :env/clojars_password
                                     :sign-releases false}]
                        ["snapshots" {:url           "https://repo.clojars.org"
                                      :username      :env/clojars_username
                                      :password      :env/clojars_password
                                      :sign-releases false}]]
  :dependencies [[org.clojure/tools.logging "1.2.4"]
                 [com.aerospike/aerospike-client "4.4.15"]
                 [funcool/promesa "8.0.450"]]
  :profiles {:dev  {:plugins        [[jonase/eastwood "0.3.5"]
                                     [lein-ancient "0.6.15"]
                                     [lein-eftest "0.5.9"]]
                    :dependencies   [[org.clojure/clojure "1.11.1"]
                                     [criterium "0.4.6"]
                                     [cheshire "5.11.0"]
                                     [com.fasterxml.jackson.core/jackson-databind "2.11.2"]
                                     [clj-kondo "2022.04.25"]]
                    :eftest         {:multithread?   false
                                     :report         eftest.report.junit/report
                                     :report-to-file "target/junit.xml"}
                    :aliases        {"clj-kondo" ["run" "-m" "clj-kondo.main"]
                                     "lint"      ["run" "-m" "clj-kondo.main" "--lint" "src" "test"]}
                    :global-vars    {*warn-on-reflection* true}
                    :test-selectors {:integration :integration
                                     :all         (constantly true)
                                     :default     (complement :integration)}}
             :docs {:plugins [[lein-codox "0.10.7"]]
                    :codox   {:output-path "codox"
                              :source-uri  "http://github.com/AppsFlyer/aerospike-clj/blob/{version}/{filepath}#L{line}"
                              :metadata    {:doc/format :markdown}}}})
