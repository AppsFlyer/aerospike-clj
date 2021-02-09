(defproject com.appsflyer/aerospike-clj "2.0.2-SNAPSHOT"
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
  :dependencies [[org.clojure/tools.logging "1.1.0"]
                 [com.aerospike/aerospike-client "5.0.3"]
                 [funcool/promesa "6.0.0"]]
  :profiles {:dev  {:plugins        [[jonase/eastwood "0.3.5"]
                                     [lein-ancient "0.6.15"]
                                     [lein-eftest "0.5.9"]]
                    :dependencies   [[org.clojure/clojure "1.10.1"]
                                     [criterium "0.4.6"]
                                     [cheshire "5.10.0"]
                                     [danlentz/clj-uuid "0.1.9"]
                                     [com.fasterxml.jackson.core/jackson-databind "2.11.2"]
                                     [clj-kondo "RELEASE"]]
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
