(ns fhofherr.clj-db-util.db
  (:import [javax.sql DataSource]))

(defn make-db
  "Create an object representing a database for use by clj-db-utils functions."
  [dialect ^DataSource ds & {:as options}]
  {::dialect dialect
   ::db-spec {:datasource ds}})

(defn db-spec
  [db]
  (::db-spec db))

(defn dialect
  [db]
  (::dialect db))
