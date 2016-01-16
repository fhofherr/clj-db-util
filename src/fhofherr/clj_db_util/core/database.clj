(ns fhofherr.clj-db-util.core.database
  (:import [com.zaxxer.hikari HikariConfig HikariDataSource]))


(defn datasource
  [url user password]
  (let [cfg (doto (HikariConfig.)
              (.setJdbcUrl url)
              (.setUsername user)
              (.setPassword password))]
    (HikariDataSource. cfg)))
