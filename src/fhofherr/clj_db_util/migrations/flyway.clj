(ns fhofherr.clj-db-util.migrations.flyway
  (:require [clojure.java.io :refer [resource]]
            [clojure.tools.logging :as log]
            [fhofherr.clj-db-util.dialect :as d]
            )
  (:import [org.flywaydb.core Flyway]))

(defn- set-datasource
  [flyway datasource url user password]
  (if (or datasource (and url user password))
    (do (when (and datasource url user password)
          (log/warnf "%s %s"
                     "A datasource as well as url, user, and password were given!"
                     "The datasource takes precedence."))
        (if datasource
          (.setDataSource flyway datasource)
          (.setDataSource flyway url user password))
        flyway)
    (log/fatal "Neither datasource nor url, user, and password given!")))

(defn- set-migrations-loc
  [flyway migration-loc]
  (if (resource migration-loc)
    (do 
      (.setLocations flyway (into-array String [migration-loc]))
      flyway)
    (log/fatalf "Migraion loc '%s' is missing!" migration-loc)))

(defn create-flyway
  "Create a new flyway instance for the given `dialect`. The database connection
  used by the returned flyway instance may be specified by passing a map
  `{:datasource d}` or a map `{:url url, :user user, :password password}`
  as the second argument.

  *Parameters*:
  - `dialect` the dialect to use
  - a map containing either a datasource or connection information"
  [dialect {:keys [datasource url user password]}]
  (some-> (Flyway.)
    (set-datasource datasource url user password)
    (set-migrations-loc (d/migrations-loc dialect))))

(defn migrate
  [dialect db-spec]
  (if-let [fw (create-flyway dialect db-spec)]
    (do
      (.migrate fw)
      db-spec)
    (log/fatalf "Could not create flyway!")))

(defn clean
  [dialect db-spec]
  (if-let [fw (create-flyway dialect db-spec)]
    (do
      (.clean fw)
      db-spec)
    (log/fatalf "Could not create flyway!")))