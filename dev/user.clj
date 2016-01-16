(ns user
  (:require [clojure.test :as t]
            [clojure.tools.namespace.repl :refer  (refresh refresh-all)]
            [clojure.repl :refer  :all]
            [clojure.java.javadoc :refer  [javadoc]]))

(defn- do-test
  []
  (t/run-all-tests #"fhofherr\.clj-db-util\.test\..+-test"))

(defn run-tests
  []
  (refresh :after 'user/do-test))
