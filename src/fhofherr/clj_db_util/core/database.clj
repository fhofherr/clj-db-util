(ns fhofherr.clj-db-util.core.database
  (:import [com.zaxxer.hikari HikariConfig HikariDataSource]))

(defrecord Database [^javax.sql.DataSource datasource])

(alter-meta! #'->Database assoc :no-doc true)
(alter-meta! #'map->Database assoc :no-doc true)

(defn new-database
  [url user password]
  (let [cfg (doto (HikariConfig.)
              (.setJdbcUrl url)
              (.setUsername user)
              (.setPassword password))
        ds (HikariDataSource. cfg)]
    (map->Database {:datasource ds})))
