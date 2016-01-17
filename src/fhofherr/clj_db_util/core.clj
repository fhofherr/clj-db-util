(ns fhofherr.clj-db-util.core
  (:require [fhofherr.clj-db-util.core.database :as database]))

(defn connect-db
  [url user password]
  {:pre [url user password]}
  {:datasource (database/datasource url user password)})

(defn verify-connection
  [db]
  {:pre [db]}
  (let [c  (.getConnection ^javax.sql.DataSource (:datasource db))]
    (try
      (.isValid c 5)
      (finally
        (.close c)))))
