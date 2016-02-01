(ns fhofherr.clj-db-util.integration.crud-test
  (:require [clojure.test :refer :all]
            [fhofherr.clj-db-util.core :as db-util]
            [environ.core :refer [env]]))

(deftest ^:integration insert!
  (db-util/with-database
    [db (db-util/connect-to-db (env :db-url) (env :db-user) (env :db-pass))]

    (testing "insert individual records"
      (db-util/clean-db db)
      (db-util/migrate-db db)

      (let [insert (db-util/insert! :t_key_value_pairs {:key "key" :value "value"})
            select (db-util/query-str "SELECT key, value FROM t_key_value_pairs")
            [_ err1] (db-util/with-db-transaction db insert)
            [res err2] (db-util/with-db-transaction db select)]
        (is (every? nil? [err1 err2]))
        (is (= [{:key "key" :value "value"}] res))))

    (testing "insert multiple records at once"
      (db-util/clean-db db)
      (db-util/migrate-db db)

      (let [insert (db-util/insert! :t_key_value_pairs
                                    {:key "key1" :value "value1"}
                                    {:key "key2" :value "value2"})
            select (db-util/query-str "SELECT key, value FROM t_key_value_pairs ORDER BY key")
            [_ err1] (db-util/with-db-transaction db insert)
            [res err2] (db-util/with-db-transaction db select)]
        (is (every? nil? [err1 err2]))
        (is (= [{:key "key1" :value "value1"} {:key "key2" :value "value2"}] res))))

    (testing "batch insert records"
      (db-util/clean-db db)
      (db-util/migrate-db db)

      (let [insert (db-util/insert! :t_key_value_pairs
                                    [:key :value]
                                    ["key1" "value1"]
                                    ["key2" "value2"])
            select (db-util/query-str "SELECT key, value FROM t_key_value_pairs ORDER BY key")
            [_ err1] (db-util/with-db-transaction db insert)
            [res err2] (db-util/with-db-transaction db select)]
        (is (every? nil? [err1 err2]))
        (is (= [{:key "key1" :value "value1"} {:key "key2" :value "value2"}] res))))))