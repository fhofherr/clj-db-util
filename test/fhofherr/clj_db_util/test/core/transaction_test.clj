(ns fhofherr.clj-db-util.test.core.transaction-test
  (:require [clojure.test :refer :all]
            [fhofherr.clj-db-util.core.database :as database]
            [fhofherr.clj-db-util.core :as db-util]
            [fhofherr.clj-db-util.core.transaction :as tx]))

(deftest transactional-operation

  (db-util/with-database
    [db (db-util/connect-db "jdbc:h2:mem:" "" "")]

    (testing "execute the body of the transactional op"
      (let [body-res (+ 1 1)
            tx-op (tx/transactional-operation [tx-state] (+ 1 1))
            [res err] (tx/with-db-transaction db tx-op)]
        (is (= res body-res))
        (is (nil? err))))

    (testing "rollback the transaction if an Exception occurs"
      (let [tx-op (tx/transactional-operation [tx-state] (throw (Exception.)))
            [res err] (tx/with-db-transaction db tx-op)]
        (is (nil? res))
        (is (= tx/err-transaction-rolled-back err))))

    (testing "call the *exception-occured* handler"
      (let [handler-args (atom nil)
            handler (fn [tx-state e]
                      (reset! handler-args [tx-state e])
                      ;; Note: does mark the transaction as rolled back.
                      ["new-result" tx-state])
            exception (Exception. "Kaboom baby!")
            tx-op (tx/transactional-operation [tx-state] (throw exception))]
        (binding [tx/*exception-occured* handler]
          (let [[res err] (tx/with-db-transaction db tx-op)]
            (is (tx/transaction-state? (first @handler-args)))
            (is (= (second @handler-args) exception))
            (is (= "new-result" res))
            (is (nil? err))))))))Implm