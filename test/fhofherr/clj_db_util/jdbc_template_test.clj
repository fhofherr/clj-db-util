(ns fhofherr.clj-db-util.jdbc-template-test
  (:require [clojure.test :refer :all]
            [fhofherr.clj-db-util.support.test-db :as test-db]
            [fhofherr.clj-db-util.jdbc-template :as t]
            [fhofherr.clj-db-util.dialect :refer [h2]]))

(use-fixtures :each (test-db/prepare-db h2 test-db/h2-in-memory))

(deftest query-str

  (testing "syntax errors"

    (let [stmt "Not SQL in any dialect."]
      (is (nil? (t/query-str test-db/*dialect*
                             test-db/*db-spec*
                             stmt)))))

  (testing "simple statements"

    (is (= {:result 1}
           (t/query-str test-db/*dialect*
                        test-db/*db-spec*
                        "SELECT 1 AS result FROM DUAL"
                        :result-set-fn first)))

    (is (= {:one 1, :two 2}
           (t/query-str test-db/*dialect*
                        test-db/*db-spec*
                        "SELECT 1 AS one, 2 AS two FROM DUAL"
                        :result-set-fn first)))

    ;; TODO Transactions!
    (do (t/insert! test-db/*dialect*
                   test-db/*db-spec*
                   :fruit
                   {:name "Apple" :cost 2.99})
        (t/insert! test-db/*dialect*
                   test-db/*db-spec*
                   :fruit_orders
                   {:fruit_id 1 :customer_name "Fruit Sales Inc."})
        (is (= {:fruit_name "Apple" :customer_name "Fruit Sales Inc."}
               (t/query-str test-db/*dialect*
                            test-db/*db-spec*
                            "SELECT f.name AS fruit_name, o.customer_name
                             FROM fruit f
                             JOIN fruit_orders o ON f.id = o.fruit_id"
                            :result-set-fn first)))

        (is (= {:fruit_name "Apple" :customer_name "Fruit Sales Inc."}
               (t/query-str test-db/*dialect*
                            test-db/*db-spec*
                            "SELECT f.name AS fruit_name, o.customer_name
                               FROM fruit f,
                                    fruit_orders o
                             WHERE f.id = o.fruit_id"
                            :result-set-fn first)))
        ))

  (testing "named-parameters"

    (testing "simple statements"
      (let [stmt "SELECT 1 AS result FROM dual WHERE 1 = :number"]
        (is (= {:result 1}
               (t/query-str test-db/*dialect*
                            test-db/*db-spec*
                            stmt
                            :params {:number 1}
                            :result-set-fn first)))
        (is (nil?
              (t/query-str test-db/*dialect*
                           test-db/*db-spec*
                           stmt
                           :params {:number 2}
                           :result-set-fn first)))))

    (testing "subselects"

      (let [stmt "SELECT 1 AS result
                 FROM dual
                 WHERE 1 = :number
                 AND EXISTS (SELECT 1 FROM DUAL WHERE 2 = :another-number)"]
        (is (= {:result 1}
               (t/query-str test-db/*dialect*
                            test-db/*db-spec*
                            stmt
                            :params {:number 1
                                     :another-number 2}
                            :result-set-fn first))))))

  (testing "template vars"
    (let [stmt "SELECT 1 AS result FROM {{ schema }}.dual"]
      (is (= {:result 1}
             (t/query-str test-db/*dialect*
                          test-db/*db-spec*
                          stmt
                          :template-vars {:schema "PUBLIC"}
                          :result-set-fn first))))))

(deftest query-res

  (testing "simple select"
    (is (= {:result 1}
           (t/query-res test-db/*dialect*
                        test-db/*db-spec*
                        "simple-select.sql"
                        :result-set-fn first)))))

(deftest insert!

  (testing "insert single row"
    (is (= [1]
           (t/insert! test-db/*dialect*
                      test-db/*db-spec*
                      :fruit
                      {:name "Apple" :cost 2.99}))))

  (testing "insert multiple rows"
    (is (= [2 3]
           (t/insert! test-db/*dialect*
                      test-db/*db-spec*
                      :fruit
                      {:name "Banana" :cost 1.85}
                      {:name "Pineapple" :cost 4.88}))))

  (testing "batch insert multiple rows"
    (is (= 2
           (t/insert! test-db/*dialect*
                      test-db/*db-spec*
                      :fruit
                      nil
                      [10 "Grapefruit" 0.76]
                      [11 "Peach" 1.56])))
    (is (= 2
           (t/insert! test-db/*dialect*
                      test-db/*db-spec*
                      :fruit
                      [:name :cost]
                      ["Strawberry" 1.79]
                      ["Raspberry" 0.56])))))
