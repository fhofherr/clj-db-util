(defproject fhofherr/clj-db-util "0.1.0-SNAPSHOT"
  :description "Various utilities for talking to your database."
  :url "https://github.com/fhofherr/clj-db-util"
  :scm {:name "git"
        :url "https://github.com/fhofherr/clj-db-util"}
  :license {:name "MIT"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/java.jdbc "0.3.6"]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.flywaydb/flyway-core "3.1"]
                 [instaparse "1.3.5"]]
  :profiles  {:dev  {:source-paths ["dev"]
                     :resource-paths ["test-resources"]
                     :plugins [[codox "0.8.10"]]
                     :codox {:output-dir "target/codox"
                             :exclude [user]
                             :defaults {:doc/format :markdown}}
                     :dependencies [[org.clojure/tools.namespace "0.2.8"]
                                    [com.h2database/h2 "1.4.184"]
                                    [org.slf4j/slf4j-api "1.7.9"]
                                    [org.slf4j/slf4j-nop "1.7.9"]]}})
