(ns fhofherr.clj-db-util.integration.transaction-test
  (:require [clojure.test :refer :all]
            [environ.core :refer [env]]
            [fhofherr.clj-db-util.core :as db-util]))

(defn insert-key-value-pair
  []
  (db-util/transactional-let [value (db-util/transactional "value")
                              inserted-rows (db-util/insert! :t-key-value-pairs {:key "key" :value value})]
    inserted-rows))

(defn read-key-value-pair
  []
  (db-util/transactional-let [query-result (db-util/query-str
                                             "SELECT key, value FROM t_key_value_pairs WHERE key = 'value'")]
    query-result))

(defn delete-key-value-pair
  []
  (db-util/transactional-let [deleted-rows (db-util/delete! :t-key-value-pairs ["key = 'value'"])]))

(deftest ^:integration simple-insert-update-delete

  (let [db (-> (db-util/connect-db (env :db-url) (env :db-user) (env :db-pass))
               (db-util/add-migrator))]

    (testing "one transaction per operation"
      (try
        ;; TODO add possibility to specify target version
        (db-util/migrate-db db)
        (let [inserted-rows (db-util/with-db-transaction (insert-key-value-pair) db)
              query-result-1 (db-util/with-db-transaction (read-key-value-pair) db)
              deleted-rows (db-util/with-db-transaction (delete-key-value-pair) db)
              query-result-2 (db-util/with-db-transaction (read-key-value-pair) db)]
          (is (= 1 inserted-rows))
          (is (= [{:key "key" :value "value"}] query-result-1))
          (is (= 1 deleted-rows))
          (is (= [] query-result-2)))
        (finally
          ;; TODO enable selection between truncating tables and wiping the whole schema
          (db-util/clean-db db))))

    (testing "chain multiple transactional operations"
      (try
        (db-util/migrate-db db)
        (let [chained (fn []
                        (db-util/transactional-let [_ (insert-key-value-pair)
                                                    query-result (read-key-value-pair)]
                                                   query-result))
              query-result (db-util/with-db-transaction (chained) db)]
          (is (= [{:key "key" :value "value"}] query-result-1)))
        (finally
          (db-util/clean-db db))))))