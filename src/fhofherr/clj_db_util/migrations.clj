(ns fhofherr.clj-db-util.migrations
  (:require [clojure.tools.logging :as log]
            [fhofherr.clj-db-util.db-connection :as db-con]
            [fhofherr.clj-db-util.migrations.flyway :as fw]))

(defn info
  [db & options]
   (log/info "Retreiving database migration information.")
   (fw/info (db-con/dialect db) (db-con/db-spec db) options))

(defn migrate
  "Migrate the database identified by `db`.
  Return the `db` representing the migrated database.

  *Parameters*:

  - `db` the database to connect to."
  [db & options]
  (log/info "Preparing to migrate database.")
  (fw/migrate (db-con/dialect db) (db-con/db-spec db) options)
  db)

(defn clean
  "Clean the database identified by`db`.
  Return a `db` representing the cleaned database.

  **Warning**: This removes all database objects! Do not call this function for
  production databases!

  *Parameters*:

  - `db` the database to clean."
  [db & options]
  (fw/clean (db-con/dialect db) (db-con/db-spec db) options)
  db)
