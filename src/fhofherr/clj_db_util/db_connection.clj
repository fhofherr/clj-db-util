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

(defn update-db-spec
  "Given a database `db` update the db-spec contained within `db`.

  **Warning**: This function is mostly for internal use. Clients should not
  need to call it! It is necessary due to the currently unfortunate
  structuring of the namespaces. It will be removed as soon as the
  namespaces have been restructured."
  {:deprecated "0.1.0"}
  [db db-spec]
  (assoc db ::db-spec db-spec))

(defn db-spec
  [db]
  (::db-spec db))

(defn dialect
  [db]
  (::dialect db))

(defn- create-con-pool
  [^DataSource ds]
  (let [cfg (doto (HikariConfig.)
              (.setDataSource ds))]
    (HikariDataSource. cfg)))

(defn wrap-pool
  "Create a connection pool and wrap the data source used to connect to
  the database into it."
  [db]
  (log/info "Creating HikariCP connection pool.")
  (if-not (::pooled? db)
    (let [ds (:datasource (db-spec db))
         pooled-ds (create-con-pool ds)]
     (as-> db $
       (update-db-spec $ {:datasource pooled-ds})
       (assoc $ ::pooled? true)))
    db))

(defn wrap-db
  "Create a connection pool and wrap the data source used to connect to
  the database into it.

  *Deprecated*: use wrap-pool instead"
  {:deprecated "0.1.0"}
  [db]
  (wrap-pool db))
 
(defn shutdown-pool
  "If the database uses a pooled data source shut it down."
  [db]
  {:pre [(:datasource (db-spec db))]}
  (if (::pooled? db)
    (let [pooled-ds (:datasource (db-spec db))
          ds (.getDataSource pooled-ds)]
      (.shutdown pooled-ds)
      (log/info "Shutdown of HikariCP connection pool successful.")
      (as-> db $
        (update-db-spec $ {:datasource ds})
        (assoc $ ::pooled? false)))
    db))
