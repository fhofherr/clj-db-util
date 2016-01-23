(ns fhofherr.clj-db-util.integration.migrate-db-test
  (:require [clojure.test :refer :all]
            [environ.core :refer [env]]
            [fhofherr.clj-db-util.core :as db-util]))

(deftest ^:integration migrate-database-to-latest-version
  (let [db (db-util/connect-db (env :db-url) (env :db-user) (env :db-pass))
        mig-result (db-util/migrate-db db)
        version (db-util/db-version db)]
    (is (= mig-result version))))