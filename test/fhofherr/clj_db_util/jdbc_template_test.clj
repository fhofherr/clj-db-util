(ns fhofherr.clj-db-util.jdbc-template-test
  (:require [clojure.test :refer :all]
            [fhofherr.clj-db-util.support.test-db :as test-db]
            [fhofherr.clj-db-util.jdbc-template :as t]
            [fhofherr.clj-db-util.dialect :refer [h2]]))

(deftest query-str

  (testing "syntax errors"

    (let [stmt "Not SQL in any dialect."]
      (is (nil? (t/query-str h2
                             (test-db/h2-private-in-memory)
                             stmt)))))

  (testing "named-parameters"

    (testing "simple statements"
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

    (testing "subselects"

      (let [stmt "SELECT 1 AS result
                 FROM dual
                 WHERE 1 = :number
                 AND EXISTS (SELECT 1 FROM DUAL WHERE 2 = :another-number)"]
        (is (= {:result 1}
               (t/query-str h2
                            (test-db/h2-private-in-memory)
                            stmt
                            :params {:number 1
                                     :another-number 2}
                            :result-set-fn first))))))

  (testing "template vars"
    (let [stmt "SELECT 1 AS result FROM {{ schema }}.dual"]
      (is (= {:result 1}
             (t/query-str h2
                          (test-db/h2-private-in-memory)
                          stmt
                          :template-vars {:schema "PUBLIC"}
                          :result-set-fn first))))))

(deftest query-res

  (testing "simple select"
    (is (= {:result 1}
           (t/query-res h2
                        (test-db/h2-private-in-memory)
                        "simple-select.sql"
                        :result-set-fn first)))))
