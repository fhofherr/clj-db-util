(ns fhofherr.clj-db-util.connection-pool
  (:require [fhofherr.clj-db-util.db :as db-con]
            [clojure.tools.logging :as log])
  (:import [javax.sql DataSource]
           [com.zaxxer.hikari HikariConfig HikariDataSource]))

(defn- create-con-pool
  [^DataSource ds]
  (let [cfg (doto (HikariConfig.)
              (.setDataSource ds))]
    (HikariDataSource. cfg)))

(defn wrap-db
  "Create a connection pool and wrap the data source used to connect to
  the database into it."
  [db]
  (log/info "Creating HikariCP connection pool.")
  (let [db-spec (db-con/db-spec db)
        dialect (db-con/dialect db)
        pooled-ds (create-con-pool (:datasource db-spec))]
    (db-con/from-datasource dialect pooled-ds :pooled true)))

(defn shutdown-pool
  "If the database uses a pooled data source shut it down."
  [db]
  {:pre [(:datasource (db-con/db-spec db))]}
  (if (db-con/pooled? db)
    (let [pool (:datasource (db-con/db-spec db))
          ds (.getDataSource pool)]
      (.shutdown pool)
      (log/info "Shutdown of HikariCP connection pool successful.")
      (db-con/from-datasource (db-con/dialect db) ds))
    db))
