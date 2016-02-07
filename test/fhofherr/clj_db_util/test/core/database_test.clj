(ns fhofherr.clj-db-util.test.core.database-test
  (:require [clojure.test :refer :all]
            [fhofherr.clj-db-util.core :as db-util]))

(deftest database-resource-locations

  (testing "default locations"
    (let [db (db-util/connect-to-db "jdbc:h2:mem:" "" "")]
      (try
        (is (= "db/h2/default/migrations" (db-util/migrations-loc db)))
        (is (= "db/h2/default/statements" (db-util/statements-loc db)))
        (finally
          (db-util/disconnect-from-db db)))))

  (testing "custom resource path"
    (let [custom-path "clj-db-util/test/db"
          db (-> (db-util/connect-to-db "jdbc:h2:mem:" "" "")
                 (assoc :db-resource-path custom-path))]
      (try
        (is (= (str custom-path "/h2/default/migrations") (db-util/migrations-loc db)))
        (is (= (str custom-path "/h2/default/statements") (db-util/statements-loc db)))
        (finally
          (db-util/disconnect-from-db db))))))