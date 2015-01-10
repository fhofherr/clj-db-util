(ns fhofherr.clj-db-util.db-connection
  (:require [clojure.tools.logging :as log])
  (:import [javax.sql DataSource]
           [com.zaxxer.hikari HikariConfig HikariDataSource]))

(defn from-datasource
  "Create an object representing a database for use by clj-db-utils functions."
  [dialect ^DataSource ds & {:as options}]
  {::dialect dialect
   ::db-spec {:datasource ds}
   ::pooled? (:pooled options false)})

(defn from-db-spec
  "Create an object representing a database from an existing db-spec for use
  by clj-db-utils functions.

  **Warning**: This function is mostly for internal use. Clients should not
  need to call it! Use [[from-datasource]] instead!"
  [dialect db-spec]
  {::dialect dialect
   ::db-spec db-spec})

(defn db-spec
  [db]
  (::db-spec db))

(defn dialect
  [db]
  (::dialect db))

(defn pooled?
  [db]
  (::pooled? db false))

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
  (let [db-spec (db-spec db)
        dialect (dialect db)
        pooled-ds (create-con-pool (:datasource db-spec))]
    (from-datasource dialect pooled-ds :pooled true)))

(defn shutdown-pool
  "If the database uses a pooled data source shut it down."
  [db]
  {:pre [(:datasource (db-spec db))]}
  (if (pooled? db)
    (let [pool (:datasource (db-spec db))
          ds (.getDataSource pool)]
      (.shutdown pool)
      (log/info "Shutdown of HikariCP connection pool successful.")
      (from-datasource (dialect db) ds))
    db))
