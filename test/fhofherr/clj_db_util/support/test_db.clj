(ns fhofherr.clj-db-util.support.test-db
  (:require [fhofherr.clj-db-util.db :as db]
            [fhofherr.clj-db-util.migrations :as migs]
            [fhofherr.clj-db-util.dialect :refer [h2]])
  (:import [org.h2.jdbcx JdbcDataSource]))

(def ^:dynamic *db* nil)
(def ^:dynamic *db-spec* nil)
(def ^:dynamic *dialect* nil)

(defn prepare-db
  [dialect db-factory & options]
  (fn [f]
    (let [db (db-factory)
          db-spec (db/db-spec db)
          dialect (db/dialect db)]
      (apply migs/migrate db options)
      (binding [*db* db
                *db-spec* db-spec
                *dialect* dialect]
        (assert *db-spec*)
        (assert *dialect*)
        (f))
      (apply migs/clean db options))))

(defn h2-in-memory
  []
  (let [ds (doto (JdbcDataSource.)
             (.setUrl "jdbc:h2:mem:h2-test;DB_CLOSE_DELAY=-1")
             (.setUser "")
             (.setPassword ""))]
    (db/make-db h2 ds)))
