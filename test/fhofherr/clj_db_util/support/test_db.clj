(ns fhofherr.clj-db-util.support.test-db
  (:import [org.h2.jdbcx JdbcDataSource]))

(defn h2-private-in-memory
  []
  (let [ds (doto (JdbcDataSource.)
             (.setUrl "jdbc:h2:mem:")
             (.setUser "")
             (.setPassword ""))]
    {:datasource ds}))
