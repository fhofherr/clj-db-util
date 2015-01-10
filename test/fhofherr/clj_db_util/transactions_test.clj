(ns fhofherr.clj-db-util.transactions-test
  (:require [clojure.test :refer :all]
            [fhofherr.clj-db-util.jdbc-template :as t]
            [fhofherr.clj-db-util.support.test-db :as test-db]
            [fhofherr.clj-db-util.transactions :as tx]))

(use-fixtures :each (test-db/prepare-db test-db/h2-in-memory
                                        :schema "TX_TEST"))

(deftest laws

  (testing "right-unit"
    (let [tx (tx/tx-return :something)]
      (is (= (tx/tx-exec test-db/*db*
                         tx)
             (tx/tx-exec test-db/*db*
                         (tx/tx-bind tx tx/tx-return))))))

  (testing "left-unit"
    (let [v 9
          f (comp tx/tx-return #(* % 10))]
      (is (= (tx/tx-exec test-db/*db*
                         (f v))
             (tx/tx-exec test-db/*db*
                         (tx/tx-bind (tx/tx-return v) f))))))

  (testing "associativity"
    (let [tx (tx/tx-return 7)
          f (fn [v] (tx/tx-return (* 31 v)))
          g (fn [v] (tx/tx-return (- v 3)))]
      (is (= (tx/tx-exec test-db/*db*
                         (tx/tx-bind (tx/tx-bind tx f) g))
             (tx/tx-exec test-db/*db*
                         (tx/tx-bind tx #(tx/tx-bind (f %) g))))))))

(deftest argument-validation
  (testing "tx-bind"
    (is (thrown? IllegalArgumentException
                 (tx/tx-exec test-db/*db*
                             (tx/tx-bind :not-a-transaction #(tx/tx-return %)))))

    (is (thrown? IllegalArgumentException
                 (tx/tx-exec test-db/*db*
                             (tx/tx-bind (tx/tx-return :something) identity))))))

(deftest bind-values
  (is (= [:a :b :c :d]
         (tx/tx-exec-> test-db/*db*
                       [a (tx/tx-return [:a])
                        b (tx/tx-return (conj a :b))
                        c (tx/tx-return (conj b :c))
                        d (tx/tx-return (conj c :d))]
                       d))))

(deftest tx-apply
  (is (= 70
         (tx/tx-exec-> test-db/*db*
                       [x (tx/tx-return 7)
                        y (tx/tx-apply #(* 10 %) x)]
                       y))))

(deftest commit

  (do (tx/tx-exec test-db/*db*
                  (t/insert! :TX_TEST.some_table
                             {:value "something"}))
      (is (= {:value "something"}
             (tx/tx-exec test-db/*db*
                         (t/query "SELECT value
                                      FROM TX_TEST.some_table"
                                      :result-set-fn first))))))

(deftest rollback
  (do (tx/tx-exec-> test-db/*db*
                    [_ (t/insert! :TX_TEST.some_table
                                  {:value "something"})
                     _ (tx/tx-rollback)
                     _ (tx/tx-apply #(throw
                                       (AssertionError.
                                         (str "You wont see me: " %)))
                                    nil)
                     _ (t/insert! :TX_TEST.some_table
                                  {:value "something else"})]))

  (is (empty? (tx/tx-exec test-db/*db*
                          (t/query "SELECT value
                                       FROM TX_TEST.some_table")))))
