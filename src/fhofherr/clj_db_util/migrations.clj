(ns fhofherr.clj-db-util.migrations
  (:require [clojure.tools.logging :as log]
            [fhofherr.clj-db-util.migrations.flyway :as fw]))

(defn migrate
  "Migrate the database identified by `db-spec` while using the dialect
  `dialect`. Return a `db-spec` representing the migrated database.

  *Parameters*:

  - `dialect` the dialect to use.
  - `db-spec` the db-spec used to connect to the database."
  [dialect db-spec]
  (log/info "Preparing to migrate database.")
  (fw/migrate dialect db-spec))

(defn clean
  "Clean the database identified by`db-spec` while using the dialect `dialect`.
  Return a `db-spec` representing the migrated database.

  **Warning**: This removes all database objects! Do not call this function for
  production databases!

  *Parameters*:

  - `dialect` the dialect to use.
  - `db-spec` the db-spec used to connect to the database."
  [dialect db-spec]
  (fw/clean dialect db-spec))
