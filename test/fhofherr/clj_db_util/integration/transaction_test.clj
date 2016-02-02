(ns fhofherr.clj-db-util.integration.transaction-test
  (:require [clojure.test :refer :all]
            [environ.core :refer [env]]
            [fhofherr.clj-db-util.core :as db-util]))

;; TODO allow keywords with spinal-case (convert to snake_case)
(def insert-key-value-pair
  (db-util/transactional-let [value (db-util/transactional "value")
                              inserted-rows (db-util/insert! :t_key_value_pairs {:key "key" :value value})]
                             inserted-rows))

(def read-key-value-pair
  (db-util/transactional-let [query-result (db-util/query-str
                                            "SELECT key, value FROM t_key_value_pairs WHERE key = 'key'")]
                             query-result))

(def delete-key-value-pair
  (db-util/transactional-let [deleted-rows (db-util/delete! :t_key_value_pairs ["key = 'key'"])]
                             deleted-rows))

(deftest ^:integration basic-transaction-handling
  (db-util/with-database
    [db (db-util/connect-to-db (env :db-url) (env :db-user) (env :db-pass))]

    (testing "one transaction per operation"
      (db-util/clean-db db)
      ;; TODO add possibility to specify target version
      (db-util/migrate-db db)

      (let [[vendor-specific err1] (db-util/with-db-transaction db insert-key-value-pair)
            [query-result-1 err2] (db-util/with-db-transaction db read-key-value-pair)
            [deleted-rows err3] (db-util/with-db-transaction db delete-key-value-pair)
            [query-result-2 err4] (db-util/with-db-transaction db read-key-value-pair)]
        (is ((complement nil?) vendor-specific))
        (is (= [{:key "key" :value "value"}] query-result-1))
        (is (= 1 deleted-rows))
        (is (empty? query-result-2))
        (is (every? nil? [err1 err2 err3 err4]))))

    (testing "chain multiple transactional operations"
      (db-util/clean-db db)
      (db-util/migrate-db db)

      (let [chained (db-util/transactional-let [_ insert-key-value-pair
                                                query-result read-key-value-pair]
                                               query-result)
            [query-result err] (db-util/with-db-transaction db chained)]
        (is (= [{:key "key" :value "value"}] query-result))
        (is (nil? err))))

    (testing "rollback"
      (db-util/clean-db db)
      (db-util/migrate-db db)

      (let [[no-result err1] (db-util/with-db-transaction db (db-util/transactional-let [_ insert-key-value-pair
                                                                                         _ (db-util/rollback!)]
                                                                                        nil))
            [query-result err2] (db-util/with-db-transaction db read-key-value-pair)]

        (is (nil? no-result))
        (is (= db-util/err-transaction-rolled-back err1))
        (is (nil? err2))
        (is (empty? query-result))))
    ))