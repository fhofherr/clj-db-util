(defproject fhofherr/clj-db-util "0.1.0-SNAPSHOT"
  :description "Various utilities for talking to your database."
  :url "https://github.com/fhofherr/clj-db-util"
  :scm {:name "git"
        :url "https://github.com/fhofherr/clj-db-util"}
  :license {:name "MIT"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.6.0"]]
  :profiles  {:dev  {:source-paths  ["dev"]
                     :dependencies  [[org.clojure/tools.namespace "0.2.7"]]}})
