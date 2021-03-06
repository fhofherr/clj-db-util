(defproject fhofherr/clj-db-util "0.2.0-SNAPSHOT"
  :description "Various utilities for talking to your database."
  :url "https://github.com/fhofherr/clj-db-util"
  :scm {:name "git"
        :url "https://github.com/fhofherr/clj-db-util"}
  :license {:name "MIT"
            :url "http://opensource.org/licenses/MIT"}
  :global-vars {*warn-on-reflection* true}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/java.jdbc "0.4.2"]
                 [org.clojure/tools.logging "0.3.1"]
                 [com.zaxxer/HikariCP "2.4.3"]
                 [org.flywaydb/flyway-core "4.0"]]
  :test-selectors {:default (complement :integration)
                   :integration :integration}
  :profiles  {:dev  {:source-paths ["dev"]
                     :resource-paths ["test-resources"]
                     :plugins [[lein-codox "0.9.1"]
                               [lein-cljfmt "0.3.0"]
                               [lein-cloverage "1.0.6"]]
                     :codox {:namespaces [#"^fhofherr\.clj-db-util\."]
                             :metadata {:doc/format :markdown}}
                     :dependencies [[environ "1.0.2"
                                     :exclusions [org.clojure/clojure]]
                                    [org.clojure/tools.namespace "0.2.10"]
                                    [org.slf4j/slf4j-api "1.7.18"]
                                    [org.slf4j/slf4j-simple "1.7.18"]
                                    [com.h2database/h2 "1.4.191"]]}
              :postgres {:dependencies [[org.postgresql/postgresql "9.4.1208"]]}
              :h2 {:dependencies [[com.h2database/h2 "1.4.191"]]}})
