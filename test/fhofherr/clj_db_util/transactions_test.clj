(ns fhofherr.clj-db-util.transactions-test
  (:require [clojure.test :refer :all]
            [fhofherr.clj-db-util.dialect :refer [h2]]
            [fhofherr.clj-db-util.jdbc-template :as t]
            [fhofherr.clj-db-util.support.test-db :as test-db]
            [fhofherr.clj-db-util.transactions :as tx]))

(use-fixtures :each (test-db/prepare-db h2
                                        test-db/h2-in-memory
                                        :schema "TX_TEST"))

(deftest laws

  (testing "right-unit"
    (let [tx (tx/tx-return :something)]
      (is (= (tx/tx-exec test-db/*dialect* test-db/*db-spec*
                         tx)
             (tx/tx-exec test-db/*dialect* test-db/*db-spec*
                         (tx/tx-bind tx tx/tx-return))))))

  (testing "left-unit"
    (let [v 9
          f (comp tx/tx-return #(* % 10))]
      (is (= (tx/tx-exec test-db/*dialect* test-db/*db-spec*
                         (f v))
             (tx/tx-exec test-db/*dialect* test-db/*db-spec*
                         (tx/tx-bind (tx/tx-return v) f))))))

  (testing "associativity"
    (let [tx (tx/tx-return 7)
          f (fn [v] (tx/tx-return (* 31 v)))
          g (fn [v] (tx/tx-return (- v 3)))]
      (is (= (tx/tx-exec test-db/*dialect* test-db/*db-spec*
                         (tx/tx-bind (tx/tx-bind tx f) g))
             (tx/tx-exec test-db/*dialect* test-db/*db-spec*
                         (tx/tx-bind tx #(tx/tx-bind (f %) g))))))))

(deftest commit

  (do (tx/tx-exec test-db/*dialect* test-db/*db-spec*
                  (t/insert! :TX_TEST.some_table
                             {:value "something"}))
      (is (= {:value "something"}
             (tx/tx-exec test-db/*dialect* test-db/*db-spec*
                         (t/query-str "SELECT value
                                      FROM TX_TEST.some_table"
                                      :result-set-fn first))))))
