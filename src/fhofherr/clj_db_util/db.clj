(ns fhofherr.clj-db-util.db
  (:import [javax.sql DataSource]))

(defn from-datasource
  "Create an object representing a database for use by clj-db-utils functions."
  [d ^DataSource ds & {:as options}]
  {::dialect d
   ::db-spec {:datasource ds}})

(defn from-db-spec
  "Create an object representing a database from an existing db-spec for use
  by clj-db-utils functions.

  **Warning**: This function is mostly for internal use. Clients should not
  need to call it! Use [[from-datasource]] instead!"
  [d dbs]
  {::dialect d
   ::db-spec dbs})

(defn db-spec
  [db]
  (::db-spec db))

(defn dialect
  [db]
  (::dialect db))
