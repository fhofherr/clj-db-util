(ns fhofherr.clj-db-util.migrations.flyway
  (:require [clojure.java.io :refer [resource]]
            [clojure.tools.logging :as log]
            [fhofherr.clj-db-util.dialect :as d]
            )
  (:import [org.flywaydb.core Flyway]
           [org.flywaydb.core.api.callback FlywayCallback]))

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
    (throw (IllegalArgumentException.
             "Neither datasource nor url, user, and password given!"))))

(defn- set-migrations-loc
  [flyway migration-loc]
  {:pre [(string? migration-loc)]}
  (if (resource migration-loc)
    (do 
      (.setLocations flyway (into-array String [migration-loc]))
      flyway)
    (throw (IllegalArgumentException.
             (format "Migraion loc '%s' is missing!" migration-loc)))))

(defn- set-schema
  [flyway schema]
  {:pre [(or (nil? schema) (string? schema))]}
  (when schema
    (.setSchemas flyway (into-array String [schema])))
  flyway)

(defn- set-placeholders
  [flyway placeholders schema]
  {:pre [(or (nil? placeholders) (map? placeholders))]}
  (letfn [(add-schema [m] (if schema
                            (assoc m "schema" schema)
                            m))
          (convert-entries [[k v]] [(name k) (str v)])]
    (.setPlaceholders flyway (as-> {} $
                                 (add-schema $)
                                 (into $ (map convert-entries placeholders)))))
  flyway)

(defn- mk-flyway-callback
  [callbacks]
  (letfn [(combine [cbs]
            {:pre [(every? fn? cbs)]}
            (fn [con & args] (doseq [cb cbs] (apply cb con args))))

          (execute [cb-key con & args]
            (when-let [cb (get callbacks cb-key)]
              (cond
                (fn? cb) (apply cb con args)
                (sequential? cb) (apply (combine cb) con args)
                :else (throw (IllegalArgumentException.
                               "Required a function or collection of functions")))))]
    (reify FlywayCallback
      (afterBaseline [this con]
        (execute :after-baseline con))
      (afterClean [this con]
        (execute :after-clean con))
      (afterEachMigrate [this con mig-info]
        (execute :after-clean con mig-info))
      (afterInfo [this con]
        (execute :after-info con))
      (afterInit [this con]
        (execute :after-init con))
      (afterMigrate [this con]
        (execute :after-migrate con))
      (afterRepair [this con]
        (execute :after-repair con))
      (afterValidate [this con]
        (execute :after-validate con))
      (beforeBaseline [this con]
        (execute :after-baseline con))
      (beforeClean [this con]
        (execute :before-clean con))
      (beforeEachMigrate [this con mig-info]
        (execute :before-each-migrate con mig-info))
      (beforeInfo [this con]
        (execute :before-info con))
      (beforeInit [this con]
        (execute :before-init con))
      (beforeMigrate [this con]
        (execute :before-migrate con))
      (beforeRepair [this con]
        (execute :before-repair con))
      (beforeValidate [this con]
        (execute :before-validate con)))))

(defn- set-callbacks
  [flyway {callbacks :callbacks}]
  {:pre [(or (nil? callbacks) (map? callbacks) (sequential? callbacks))]}
  (when callbacks
    (let [cbs (if (map? callbacks) [callbacks] callbacks)
          cb-array (into-array FlywayCallback (map mk-flyway-callback cbs))]
      (.setCallbacks flyway cb-array)))
  flyway)

(defn create-flyway
  "Create a new flyway instance for the given `dialect`. The database connection
  used by the returned flyway instance may be specified by passing a map
  `{:datasource d}` or a map `{:url url, :user user, :password password}`
  as the second argument.

  *Parameters*:
  - `dialect` the dialect to use
  - a map containing either a datasource or connection information"
  [dialect {:keys [datasource url user password]} {:keys [schema
                                                          placeholders]
                                                   :as options}]
  (-> (Flyway.)
    (set-datasource datasource url user password)
    (set-schema schema)
    (set-placeholders placeholders schema)
    (set-callbacks options)
    (set-migrations-loc (d/migrations-loc dialect schema))))


(defn- get-info
  [fw]
  (letfn [(convert-info [is] (map bean is))]
    (let [mig-info-service (.info fw)]
     {:pending (convert-info (.pending mig-info-service))
      :applied (convert-info (.applied mig-info-service))
      :current (convert-info (.current mig-info-service))})))

(defn- has-pending-migrations?
  [fw]
  (-> fw
      (get-info)
      (:pending)
      (not-empty)))

(defn info
  [dialect db-spec options]
  (let [fw (create-flyway dialect db-spec options)]
      (get-info fw)))

(defn migrate
  [dialect db-spec options]
  (let [fw (create-flyway dialect db-spec options)]
    (when (has-pending-migrations? fw)
      (do
        (.migrate fw)
        db-spec))))

(defn clean
  [dialect db-spec options]
  (let [fw (create-flyway dialect db-spec options)]
    (do
      (.clean fw)
      db-spec)))
