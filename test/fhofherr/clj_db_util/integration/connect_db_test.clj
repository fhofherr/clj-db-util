(ns fhofherr.clj-db-util.integration.connect-db-test
  (:require [clojure.test :refer :all]
            [environ.core :refer [env]]
            [fhofherr.clj-db-util.core :as db-util]))

(deftest ^:integration define-database-connection
  (let [url (env :db-url)
        user (env :db-user)
        password (env :db-pass)
        db (db-util/connect-to-db url user password)]
    (try
      (is (true? (db-util/verify-connection db)))
      (finally
        (db-util/disconnect-from-db db)))))
