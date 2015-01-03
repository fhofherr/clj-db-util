(ns fhofherr.clj-db-util.support.test-db
  (:require [fhofherr.clj-db-util.migrations :as migs]
            [fhofherr.clj-db-util.dialect :refer [h2]])
  (:import [org.h2.jdbcx JdbcDataSource]))

(def ^:dynamic *db-spec* nil)
(def ^:dynamic *dialect* nil)

(defn prepare-db
  [dialect db-spec-factory]
  (let [db-spec (db-spec-factory)]
    (fn [f]
      (binding [*db-spec* (migs/migrate dialect db-spec)
                *dialect* dialect]
        (assert *db-spec*)
        (f)
        (migs/clean dialect db-spec)))))

(defn h2-in-memory
  []
  (let [ds (doto (JdbcDataSource.)
             (.setUrl "jdbc:h2:mem:h2-test;DB_CLOSE_DELAY=-1")
             (.setUser "")
             (.setPassword ""))]
    {:datasource ds}))
