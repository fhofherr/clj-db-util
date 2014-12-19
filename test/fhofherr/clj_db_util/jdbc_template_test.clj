(ns fhofherr.clj-db-util.jdbc-template-test
  (:require [clojure.test :refer :all]
            [fhofherr.clj-db-util.support.test-db :as test-db]
            [fhofherr.clj-db-util.jdbc-template :as t]))


(deftest simple-select
  (is (= {:result 1}
         (t/query-str :h2
                      (test-db/h2-private-in-memory)
                      "SELECT 1 AS result FROM DUAL"
                      :result-set-fn first))))
