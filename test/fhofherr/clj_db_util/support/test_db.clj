(ns fhofherr.clj-db-util.support.test-db
  (:require [fhofherr.clj-db-util.db-connection :as db-con]
            [fhofherr.clj-db-util.migrations :as migs]
            [fhofherr.clj-db-util.dialect :refer [h2]])
  (:import [org.h2.jdbcx JdbcDataSource]))

(def ^:dynamic *db* nil)

(defn prepare-db
  [db-factory & options]
  (fn [f]
    (let [db (as-> (db-factory) $
               (apply migs/migrate $ options)
               (db-con/wrap-db $))]
      (binding [*db* db]
        (f))
      (as-> db $
        (db-con/shutdown-pool $)
        (apply migs/clean $ options)))))

(defn h2-in-memory
  []
  (let [ds (doto (JdbcDataSource.)
             (.setUrl "jdbc:h2:mem:h2-test;DB_CLOSE_DELAY=-1")
             (.setUser "")
             (.setPassword ""))]
    (db-con/from-datasource h2 ds)))
