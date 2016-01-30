(ns fhofherr.clj-db-util.test.core.transaction-test
  (:require [clojure.test :refer :all]
            [fhofherr.clj-db-util.core :as db-util]))

(deftest transactional-operation

  (db-util/with-database
    [db (db-util/connect-to-db "jdbc:h2:mem:" "" "")]

    (testing "execute the body of the transactional op"
      (let [body-res (+ 1 1)
            tx-op (db-util/transactional-operation [tx-state] [(+ 1 1) tx-state])
            [res err] (db-util/with-db-transaction db tx-op)]
        (is (= res body-res))
        (is (nil? err))))

    (testing "rollback the transaction if an Exception occurs"
      (let [tx-op (db-util/transactional-operation [tx-state] (throw (Exception.)))
            [res err] (db-util/with-db-transaction db tx-op)]
        (is (nil? res))
        (is (= db-util/err-transaction-rolled-back err))))

    (testing "call the *exception-during-transaction* handler"
      (let [handler-args (atom nil)
            handler (fn [tx-state e]
                      (reset! handler-args [tx-state e])
                      ;; Note: does mark the transaction as rolled back.
                      ["new-result" tx-state])
            exception (Exception. "Kaboom baby!")
            tx-op (db-util/transactional-operation [tx-state] (throw exception))]
        (binding [db-util/*exception-during-transaction* handler]
          (let [[res err] (db-util/with-db-transaction db tx-op)]
            (is (db-util/transaction-state? (first @handler-args)))
            (is (= (second @handler-args) exception))
            (is (= "new-result" res))
            (is (nil? err))))))))

(deftest transactional
  (db-util/with-database
    [db (db-util/connect-to-db "jdbc:h2:mem:" "" "")]

    (testing "wraps a form into a transactional operation"
      (let [something "something"
            tx-op (db-util/transactional something)
            [res err] (db-util/with-db-transaction db tx-op)]
        (is (= something res))
        (is (nil? err))))

    (testing "the form is only evaluated within a transaction"
      (let [something (atom 1)
            tx-op (db-util/transactional (swap! something inc))]
        (is (= 1 @something))
        (let [[res err] (db-util/with-db-transaction db tx-op)]
          (is (= 2 @something))
          (is (nil? err)))))))

(deftest transactional-bind
  (db-util/with-database
    [db (db-util/connect-to-db "jdbc:h2:mem:" "" "")]

    (testing "chain transactional operations"
      (let [tx-op1 (db-util/transactional [:tx-op-1])
            tx-op2 (db-util/transactional-bind tx-op1 #(db-util/transactional (conj % :tx-op-2)))
            [res err] (db-util/with-db-transaction db tx-op2)]
        (is (= [:tx-op-1 :tx-op-2] res))
        (is (nil? err))))))