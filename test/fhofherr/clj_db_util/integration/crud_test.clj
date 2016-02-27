(ns fhofherr.clj-db-util.integration.crud-test
  (:require [clojure.test :refer :all]
            [clojure.string :as string]
            [fhofherr.clj-db-util.core :as db-util]
            [environ.core :refer [env]]))

(def insert (db-util/insert! :t_key_value_pairs {:key "key" :value "value"}))
(def select (db-util/query "SELECT key, value FROM t_key_value_pairs ORDER BY key"))

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

(deftest ^:integration insert-and-convert-table-names
  (db-util/with-database
    [db (db-util/connect-to-db (env :db-url) (env :db-user) (env :db-pass) {:to-db-name #(.replace % \- \_)})]

    (testing "insert multiple records at once"
      (db-util/clean-db db)
      (db-util/migrate-db db)

      (let [multi-insert (db-util/insert! :t-key-value-pairs
                                          {:key "key1" :value "value1"}
                                          {:key "key2" :value "value2"})
            [_ err1] (db-util/with-db-transaction db multi-insert)
            [res err2] (db-util/with-db-transaction db select)]
        (is (every? nil? [err1 err2]))
        (is (= [{:key "key1" :value "value1"} {:key "key2" :value "value2"}] res))))

    (testing "batch insert records"
      (db-util/clean-db db)
      (db-util/migrate-db db)

      (let [batch-insert (db-util/insert! :t-key-value-pairs
                                          [:key :value]
                                          ["key1" "value1"]
                                          ["key2" "value2"])
            [_ err1] (db-util/with-db-transaction db batch-insert)
            [res err2] (db-util/with-db-transaction db select)]
        (is (every? nil? [err1 err2]))
        (is (= [{:key "key1" :value "value1"} {:key "key2" :value "value2"}] res))))))

#_(deftest ^:integration insert-and-convert-column-names
  (db-util/with-database
    [db (db-util/connect-to-db (env :db-url) (env :db-user) (env :db-pass) {:from-db-name #(.replace % \- \_)})]

    (testing "insert multiple records at once"
      (db-util/clean-db db)
      (db-util/migrate-db db)

      (let [multi-insert (db-util/insert! :t_key_value_pairs
                                          {:k-e-y "key1" :value "value1"}
                                          {:key "key2" :v-a-l-u-e "value2"})
            [_ err1] (db-util/with-db-transaction db multi-insert)
            [res err2] (db-util/with-db-transaction db select)]
        (is (every? nil? [err1 err2]))
        (is (= [{:key "key1" :value "value1"} {:key "key2" :value "value2"}] res))))

    (testing "batch insert records"
      (db-util/clean-db db)
      (db-util/migrate-db db)

      (let [batch-insert (db-util/insert! :t_key_value_pairs
                                          [:k-e-y :v-a-l-u-e]
                                          ["key1" "value1"]
                                          ["key2" "value2"])
            [_ err1] (db-util/with-db-transaction db batch-insert)
            [res err2] (db-util/with-db-transaction db select)]
        (is (every? nil? [err1 err2]))
        (is (= [{:key "key1" :value "value1"} {:key "key2" :value "value2"}] res))))))

(deftest ^:integration query
  (db-util/with-database
    [db (db-util/connect-to-db (env :db-url) (env :db-user) (env :db-pass))]

    (testing "select without parameters"
      (db-util/clean-db db)
      (db-util/migrate-db db)

      (let [select-no-params (db-util/query "SELECT key, value FROM t_key_value_pairs")
            [_ err1] (db-util/with-db-transaction db insert)
            [res err2] (db-util/with-db-transaction db select-no-params)]
        (is (every? nil? [err1 err2]))
        (is (= [{:key "key" :value "value"}] res))))

    (testing "select with positional parameters"
      (db-util/clean-db db)
      (db-util/migrate-db db)

      (let [select-pos-params (db-util/query "SELECT key, value FROM t_key_value_pairs WHERE key = ?"
                                             ["key"])
            [_ err1] (db-util/with-db-transaction db insert)
            [res err2] (db-util/with-db-transaction db select-pos-params)]
        (is (every? nil? [err1 err2]))
        (is (= [{:key "key" :value "value"}] res))))

    (testing "select with named parameters"
      (db-util/clean-db db)
      (db-util/migrate-db db)

      (let [select-named-params (db-util/query "SELECT key, value FROM t_key_value_pairs WHERE key = :key"
                                               {:key "key"})
            [_ err1] (db-util/with-db-transaction db insert)
            [res err2] (db-util/with-db-transaction db select-named-params)]
        (is (every? nil? [err1 err2]))
        (is (= [{:key "key" :value "value"}] res))))))

(deftest ^:integration query-and-convert-column-names
  (db-util/with-database
    [db (db-util/connect-to-db (env :db-url) (env :db-user) (env :db-pass) {:from-db-name string/upper-case})]
    (testing "select without parameters"
      (db-util/clean-db db)
      (db-util/migrate-db db)

      (let [select-no-params (db-util/query "SELECT key, value FROM t_key_value_pairs")
            [_ err1] (db-util/with-db-transaction db insert)
            [res err2] (db-util/with-db-transaction db select-no-params)]
        (is (every? nil? [err1 err2]))
        (is (= [{:KEY "key" :VALUE "value"}] res))))))

(deftest ^:integration update!
  (db-util/with-database
    [db (db-util/connect-to-db (env :db-url) (env :db-user) (env :db-pass))]

    (testing "unconditional update"
      (db-util/clean-db db)
      (db-util/migrate-db db)

      (let [update (db-util/update! :t_key_value_pairs {:value "another value"})
            [_ err1] (db-util/with-db-transaction db insert)
            [n-updated err2] (db-util/with-db-transaction db update)
            [res err3] (db-util/with-db-transaction db select)]
        (is (every? nil? [err1 err2 err3]))
        (is (= [1] n-updated))
        (is (= [{:key "key" :value "another value"}] res))))

    (testing "update with where clause"
      (db-util/clean-db db)
      (db-util/migrate-db db)

      (let [update (db-util/update! :t_key_value_pairs {:value "another value"} "key = 'key'")
            [_ err1] (db-util/with-db-transaction db insert)
            [n-updated err2] (db-util/with-db-transaction db update)
            [res err3] (db-util/with-db-transaction db select)]
        (is (every? nil? [err1 err2 err3]))
        (is (= [1] n-updated))
        (is (= [{:key "key" :value "another value"}] res))))

    (testing "update with positional parameters"
      (db-util/clean-db db)
      (db-util/migrate-db db)

      (let [update (db-util/update! :t_key_value_pairs {:value "another value"} "key = ?" ["key"])
            [_ err1] (db-util/with-db-transaction db insert)
            [n-updated err2] (db-util/with-db-transaction db update)
            [res err3] (db-util/with-db-transaction db select)]
        (is (every? nil? [err1 err2 err3]))
        (is (= [1] n-updated))
        (is (= [{:key "key" :value "another value"}] res))))

    (testing "update with named parameters"
      (db-util/clean-db db)
      (db-util/migrate-db db)

      (let [update (db-util/update! :t_key_value_pairs {:value "another value"} "key = :key" {:key "key"})
            [_ err1] (db-util/with-db-transaction db insert)
            [n-updated err2] (db-util/with-db-transaction db update)
            [res err3] (db-util/with-db-transaction db select)]
        (is (every? nil? [err1 err2 err3]))
        (is (= [1] n-updated))
        (is (= [{:key "key" :value "another value"}] res))))))

(deftest ^:integration update-and-convert-table-names
  (db-util/with-database
    [db (db-util/connect-to-db (env :db-url) (env :db-user) (env :db-pass) {:to-db-name #(.replace % \- \_)})]

    (testing "unconditional update"
      (db-util/clean-db db)
      (db-util/migrate-db db)

      (let [update (db-util/update! :t-key-value-pairs {:value "another value"})
            [_ err1] (db-util/with-db-transaction db insert)
            [n-updated err2] (db-util/with-db-transaction db update)
            [res err3] (db-util/with-db-transaction db select)]
        (is (every? nil? [err1 err2 err3]))
        (is (= [1] n-updated))
        (is (= [{:key "key" :value "another value"}] res))))))

#_(deftest ^:integration update-and-convert-column-names
  (db-util/with-database
    [db (db-util/connect-to-db (env :db-url) (env :db-user) (env :db-pass) {:from-db-name #(.replace % "-" "")})]

    (testing "unconditional update"
      (db-util/clean-db db)
      (db-util/migrate-db db)

      (let [update (db-util/update! :t_key_value_pairs {:v-a-l-u-e "another value"})
            [_ err1] (db-util/with-db-transaction db insert)
            [n-updated err2] (db-util/with-db-transaction db update)
            [res err3] (db-util/with-db-transaction db select)]
        (is (every? nil? [err1 err2 err3]))
        (is (= [1] n-updated))
        (is (= [{:key "key" :value "another value"}] res))))))

(deftest ^:integration delete!
  (db-util/with-database
    [db (db-util/connect-to-db (env :db-url) (env :db-user) (env :db-pass))]

    (testing "unconditional delete"
      (db-util/clean-db db)
      (db-util/migrate-db db)

      (let [delete (db-util/delete! :t_key_value_pairs)
            [_ err1] (db-util/with-db-transaction db insert)
            [n-deleted err2] (db-util/with-db-transaction db delete)
            [res err3] (db-util/with-db-transaction db select)]
        (is (every? nil? [err1 err2 err3]))
        (is (= [1] n-deleted))
        (is (empty? res))))

    (testing "conditional delete"
      (db-util/clean-db db)
      (db-util/migrate-db db)
      (let [delete (db-util/delete! :t_key_value_pairs "key = 'key'")
            [_ err1] (db-util/with-db-transaction db insert)
            [n-deleted err2] (db-util/with-db-transaction db delete)
            [res err3] (db-util/with-db-transaction db select)]
        (is (every? nil? [err1 err2 err3]))
        (is (= [1] n-deleted))
        (is (empty? res))))

    (testing "conditional delete with positional parameters"
      (db-util/clean-db db)
      (db-util/migrate-db db)
      (let [delete (db-util/delete! :t_key_value_pairs "key = ?" ["key"])
            [_ err1] (db-util/with-db-transaction db insert)
            [n-deleted err2] (db-util/with-db-transaction db delete)
            [res err3] (db-util/with-db-transaction db select)]
        (is (every? nil? [err1 err2 err3]))
        (is (= [1] n-deleted))
        (is (empty? res))))

    (testing "conditional delete with named parameters"
      (db-util/clean-db db)
      (db-util/migrate-db db)
      (let [delete (db-util/delete! :t_key_value_pairs "key = :key" {:key "key"})
            [_ err1] (db-util/with-db-transaction db insert)
            [n-deleted err2] (db-util/with-db-transaction db delete)
            [res err3] (db-util/with-db-transaction db select)]
        (is (every? nil? [err1 err2 err3]))
        (is (= [1] n-deleted))
        (is (empty? res))))))

(deftest ^:integration delete-and-convert-column-names
  (db-util/with-database
    [db (db-util/connect-to-db (env :db-url) (env :db-user) (env :db-pass) {:from-db-name #(.replace % "-" "")})]

    ))

(deftest ^:integration execute!
  (db-util/with-database
    [db (db-util/connect-to-db (env :db-url) (env :db-user) (env :db-pass))]

    (testing "execute! without parameters"
      (db-util/clean-db db)
      (db-util/migrate-db db)

      (let [execute-insert (db-util/execute! "INSERT INTO t_key_value_pairs (key, value) values ('key', 'value')")
            [n-inserted err1] (db-util/with-db-transaction db execute-insert)
            [res err2] (db-util/with-db-transaction db select)]
        (is (every? nil? [err1 err2]))
        (is (= [1] n-inserted))
        (is (= [{:key "key" :value "value"}] res))))

    (testing "execute! with positional parameters"
      (db-util/clean-db db)
      (db-util/migrate-db db)
      (let [execute-update (db-util/execute! "UPDATE t_key_value_pairs SET value = 'another value' WHERE key = ?"
                                             ["key"])
            [_ err1] (db-util/with-db-transaction db insert)
            [n-updated err2] (db-util/with-db-transaction db execute-update)
            [res err3] (db-util/with-db-transaction db select)]
        (is (every? nil? [err1 err2 err3]))
        (is (= [1] n-updated))
        (is (= [{:key "key" :value "another value"}] res))))

    (testing "execute! with named parameters"
      (db-util/clean-db db)
      (db-util/migrate-db db)
      (let [execute-update (db-util/execute! "UPDATE t_key_value_pairs SET value = 'another value' WHERE key = :key"
                                             {:key "key"})
            [_ err1] (db-util/with-db-transaction db insert)
            [n-updated err2] (db-util/with-db-transaction db execute-update)
            [res err3] (db-util/with-db-transaction db select)]
        (is (every? nil? [err1 err2 err3]))
        (is (= [1] n-updated))
        (is (= [{:key "key" :value "another value"}] res)))

      (let [execute-insert (db-util/execute! "INSERT INTO t_key_value_pairs(key, value) values (:key, :value);"
                                             {:key "key15" :value "and another value"})
            [n-inserted err1] (db-util/with-db-transaction db execute-insert)]
        (is (every? nil? err1))
        (is (= [1] n-inserted))))))

(deftest ^:integration execute-and-convert-column-names
  (db-util/with-database
    [db (db-util/connect-to-db (env :db-url) (env :db-user) (env :db-pass) {:from-db-name string/upper-case})]

    (testing "execute! without parameters"
      (db-util/clean-db db)
      (db-util/migrate-db db)

      (let [execute-insert (db-util/execute! "INSERT INTO t_key_value_pairs (key, value) values ('key', 'value')")
            [n-inserted err1] (db-util/with-db-transaction db execute-insert)
            [res err2] (db-util/with-db-transaction db select)]
        (is (every? nil? [err1 err2]))
        (is (= [1] n-inserted))
        (is (= [{:KEY "key" :VALUE "value"}] res))))))