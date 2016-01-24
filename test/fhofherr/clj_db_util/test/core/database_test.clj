(ns fhofherr.clj-db-util.test.core.database-test
  (:require [clojure.test :refer :all]
            [fhofherr.clj-db-util.core.database :as database]))

(deftest database-resource-locations

  (testing "default locations"
    (let [db (database/new-database "jdbc:h2:mem:" "" "")]
      (try
        (is (= "classpath:/db/h2/default/migrations" (database/migrations-loc db)))
        (is (= "classpath:/db/h2/default/statements" (database/statements-loc db)))
        (finally
          (database/close db)))))

  (testing "custom resource path"
    (let [custom-path "classpath:/clj-db-util/test/db"
          db (-> (database/new-database "jdbc:h2:mem:" "" "")
                 (assoc :db-resource-path custom-path))]
      (try
        (is (= (str custom-path "/h2/default/migrations") (database/migrations-loc db)))
        (is (= (str custom-path "/h2/default/statements") (database/statements-loc db)))
        (finally
          (database/close db))))))