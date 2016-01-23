(ns fhofherr.clj-db-util.core
  (:require [fhofherr.clj-db-util.core.database :as database]))

(defn connect-db
  [url user password]
  {:pre [url user password]}
  (database/new-database url user password))

(defn migrate-db
  [db])

(defn db-version
  [db])

(defn verify-connection
  [db]
  {:pre [db]}
  (let [c  (.getConnection ^javax.sql.DataSource (:datasource db))]
    (try
      (.isValid c 5)
      (finally
        (.close c)))))
