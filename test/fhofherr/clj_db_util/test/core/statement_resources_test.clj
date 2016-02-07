(ns fhofherr.clj-db-util.test.core.statement-resources-test
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [fhofherr.clj-db-util.core :as db-util]))

(deftest load-stmt
  (db-util/with-database
    [db (db-util/connect-to-db "jdbc:h2:mem:" "" "")]

    (testing "load statement as resource"
      (let [expected-stmt (-> "db/h2/default/statements/select_kvp.sql"
                              (io/resource)
                              (slurp))
            [actual-stmt err] (db-util/with-db-transaction db (db-util/load-stmt "select_kvp.sql"))]
        (is (nil? err))
        (is (= expected-stmt actual-stmt))))

    (testing "throws ex-info if resource could not be found"
      (let [[res err] (db-util/with-db-transaction db (db-util/load-stmt "not-there.sql"))]
        (is (nil? res))
        (is (= db-util/err-transaction-rolled-back err))))))