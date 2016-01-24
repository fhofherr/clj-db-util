(ns fhofherr.clj-db-util.core
  (:require [fhofherr.clj-db-util.core.database :as database])
  (:import (javax.sql DataSource)))

(defn connect-db
  [url user password]
  {:pre [url user password]}
  (database/new-database url user password))

(defn add-migrator
  [db]
  (database/add-migrator db))

(defn migrate-db
  [db]
  (database/migrate db))

(defn db-version
  [db]
  (database/version db))

(defn verify-connection
  [db]
  {:pre [db]}
  (let [c  (.getConnection ^DataSource (:datasource db))]
    (try
      (.isValid c 5)
      (finally
        (.close c)))))

(defn close-db
  [db]
  {:pre [db]}
  (database/close db))
