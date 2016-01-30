(ns fhofherr.clj-db-util.integration.transaction-test
  (:require [clojure.test :refer :all]
            [environ.core :refer [env]]
            [fhofherr.clj-db-util.core :as db-util]))

(def insert-key-value-pair
  (db-util/transactional-let [value (db-util/transactional "value")
                              inserted-rows (db-util/insert! :t-key-value-pairs {:key "key" :value value})]
                             inserted-rows))

(def read-key-value-pair
  (db-util/transactional-let [query-result (db-util/query-str
                                            "SELECT key, value FROM t_key_value_pairs WHERE key = 'value'")]
                             query-result))

(def delete-key-value-pair
  (db-util/transactional-let [deleted-rows (db-util/delete! :t-key-value-pairs ["key = 'value'"])]
                             deleted-rows))

(deftest ^:integration simple-insert-update-delete

  (db-util/with-database
    [db (db-util/connect-to-db (env :db-url) (env :db-user) (env :db-pass))]

    (testing "one transaction per operation"
      (try
        ;; TODO add possibility to specify target version
        (db-util/migrate-db db)

        (println db)

        (let [[inserted-rows err1] (db-util/with-db-transaction db insert-key-value-pair)
              [query-result-1 err2] (db-util/with-db-transaction db read-key-value-pair)
              [deleted-rows err3] (db-util/with-db-transaction db delete-key-value-pair)
              [query-result-2 err4] (db-util/with-db-transaction db read-key-value-pair)]
          (is (= 1 inserted-rows))
          (is (= [{:key "key" :value "value"}] query-result-1))
          (is (= 1 deleted-rows))
          (is (= [] query-result-2))
          (is (every? nil? [err1 err2 err3 err4])))

        (finally
          ;; TODO enable selection between truncating tables and wiping the whole schema
          (db-util/clean-db db))))

    (testing "chain multiple transactional operations"
      (try
        (db-util/migrate-db db)

        (let [chained (db-util/transactional-let [_ insert-key-value-pair
                                                  query-result read-key-value-pair]
                                                 query-result)
              [query-result err] (db-util/with-db-transaction db chained)]
          (is (= [{:key "key" :value "value"}] query-result))
          (is (nil? err)))

        (finally
          (db-util/clean-db db))))))