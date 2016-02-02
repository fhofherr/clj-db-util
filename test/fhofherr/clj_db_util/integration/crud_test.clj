(ns fhofherr.clj-db-util.integration.crud-test
  (:require [clojure.test :refer :all]
            [fhofherr.clj-db-util.core :as db-util]
            [environ.core :refer [env]]))

(def insert (db-util/insert! :t_key_value_pairs {:key "key" :value "value"}))
(def select (db-util/query-str "SELECT key, value FROM t_key_value_pairs ORDER BY key"))

(deftest ^:integration insert!
  (db-util/with-database
    [db (db-util/connect-to-db (env :db-url) (env :db-user) (env :db-pass))]

    (testing "insert individual records"
      (db-util/clean-db db)
      (db-util/migrate-db db)

      (let [[_ err1] (db-util/with-db-transaction db insert)
            [res err2] (db-util/with-db-transaction db select)]
        (is (every? nil? [err1 err2]))
        (is (= [{:key "key" :value "value"}] res))))

    (testing "insert multiple records at once"
      (db-util/clean-db db)
      (db-util/migrate-db db)

      (let [multi-insert (db-util/insert! :t_key_value_pairs
                                          {:key "key1" :value "value1"}
                                          {:key "key2" :value "value2"})
            [_ err1] (db-util/with-db-transaction db multi-insert)
            [res err2] (db-util/with-db-transaction db select)]
        (is (every? nil? [err1 err2]))
        (is (= [{:key "key1" :value "value1"} {:key "key2" :value "value2"}] res))))

    (testing "batch insert records"
      (db-util/clean-db db)
      (db-util/migrate-db db)

      (let [batch-insert (db-util/insert! :t_key_value_pairs
                                          [:key :value]
                                          ["key1" "value1"]
                                          ["key2" "value2"])
            [_ err1] (db-util/with-db-transaction db batch-insert)
            [res err2] (db-util/with-db-transaction db select)]
        (is (every? nil? [err1 err2]))
        (is (= [{:key "key1" :value "value1"} {:key "key2" :value "value2"}] res))))))

(deftest ^:integration update!
  (db-util/with-database
    [db (db-util/connect-to-db (env :db-url) (env :db-user) (env :db-pass))]

    (testing "basic update"
      (db-util/clean-db db)
      (db-util/migrate-db db)

      (let [update (db-util/update! :t_key_value_pairs {:value "another value"} ["key = 'key'"])
            [_ err1] (db-util/with-db-transaction db insert)
            [n-updated err2] (db-util/with-db-transaction db update)
            [res err3] (db-util/with-db-transaction db select)]
        (is (every? nil? [err1 err2 err3]))
        (is (= 1 n-updated))
        (is (= [{:key "key" :value "another value"}] res))))

    (testing "update with positional parameters"
      (db-util/clean-db db)
      (db-util/migrate-db db)

      (let [update (db-util/update! :t_key_value_pairs {:value "another value"} ["key = ?" "key"])
            [_ err1] (db-util/with-db-transaction db insert)
            [n-updated err2] (db-util/with-db-transaction db update)
            [res err3] (db-util/with-db-transaction db select)]
        (is (every? nil? [err1 err2 err3]))
        (is (= 1 n-updated))
        (is (= [{:key "key" :value "another value"}] res))))))

(deftest ^:integration delete!
  (db-util/with-database
    [db (db-util/connect-to-db (env :db-url) (env :db-user) (env :db-pass))]

    (testing "unconditional delete"
      (db-util/clean-db db)
      (db-util/migrate-db db)

      (let [delete (db-util/delete! :t_key_value_pairs [])
            [_ err1] (db-util/with-db-transaction db insert)
            [n-deleted err2] (db-util/with-db-transaction db delete)
            [res err3] (db-util/with-db-transaction db select)]
        (is (every? nil? [err1 err2 err3]))
        (is (= 1 n-deleted))
        (is (empty? res))))

    (testing "conditional delete"
      (db-util/clean-db db)
      (db-util/migrate-db db)
      (let [delete (db-util/delete! :t_key_value_pairs ["key = 'key'"])
            [_ err1] (db-util/with-db-transaction db insert)
            [n-deleted err2] (db-util/with-db-transaction db delete)
            [res err3] (db-util/with-db-transaction db select)]
        (is (every? nil? [err1 err2 err3]))
        (is (= 1 n-deleted))
        (is (empty? res))))

    (testing "conditional delete with positional parameters"
      (db-util/clean-db db)
      (db-util/migrate-db db)
      (let [delete (db-util/delete! :t_key_value_pairs ["key = ?", "key"])
            [_ err1] (db-util/with-db-transaction db insert)
            [n-deleted err2] (db-util/with-db-transaction db delete)
            [res err3] (db-util/with-db-transaction db select)]
        (is (every? nil? [err1 err2 err3]))
        (is (= 1 n-deleted))
        (is (empty? res))))))