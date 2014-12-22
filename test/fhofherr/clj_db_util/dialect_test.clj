(ns fhofherr.clj-db-util.dialect-test
  (:require [clojure.test :refer :all]
            [fhofherr.clj-db-util.dialect :as d]))

(deftest parse
  (testing "syntax errors"
    (is (nil? (d/parse d/h2 "Not SQL in any dialect")))))
