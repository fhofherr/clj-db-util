(ns fhofherr.clj-db-util.support.test-db
  (:require [fhofherr.clj-db-util.migrations :as migs]
            [fhofherr.clj-db-util.dialect :refer [h2]])
  (:import [org.h2.jdbcx JdbcDataSource]))

(defn h2-private-in-memory
  []
  (let [ds (doto (JdbcDataSource.)
             (.setUrl "jdbc:h2:mem:")
             (.setUser "")
             (.setPassword ""))]
    (some->> {:datasource ds}
            (migs/migrate h2))))
