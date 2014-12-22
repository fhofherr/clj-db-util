(ns fhofherr.clj-db-util.jdbc-template-test
  (:require [clojure.test :refer :all]
            [fhofherr.clj-db-util.support.test-db :as test-db]
            [fhofherr.clj-db-util.jdbc-template :as t]
            [fhofherr.clj-db-util.dialect :refer [h2]]))


(deftest named-parameters
  (let [stmt "SELECT 1 AS result FROM dual WHERE 1 = :number"]
    (is (= {:result 1}
         (t/query-str h2
                      (test-db/h2-private-in-memory)
                      stmt
                      :params {:number 1}
                      :result-set-fn first)))
    (is (nil?
         (t/query-str h2
                      (test-db/h2-private-in-memory)
                      stmt
                      :params {:number 2}
                      :result-set-fn first)))))
