(ns fhofherr.clj-db-util.core.database
  (:import [com.zaxxer.hikari HikariConfig HikariDataSource]
           (javax.sql DataSource)
           (org.flywaydb.core Flyway)))

(defrecord Database [^DataSource datasource
                     ^String db-resource-path
                     ^String schema
                     ^String vendor
                     ^Flyway migrator])

(alter-meta! #'->Database assoc :no-doc true)
(alter-meta! #'map->Database assoc :no-doc true)

(defn- vendor-from-url
  [^String url]
  (cond
    (.startsWith url "jdbc:h2") :h2
    (.startsWith url "jdbc:postgresql") :postgres
    :else (throw (ex-info (format "Can't determine vendor for database url: %s" url)
                          {:cause #{:unknown-database-vendor}
                           :url url}))))

(defn new-database
  [^String url ^String user ^String password]
  (let [cfg (doto (HikariConfig.)
              (.setJdbcUrl url)
              (.setUsername user)
              (.setPassword password))
        ds (HikariDataSource. cfg)]
    (let [db-cfg {:db-resource-path "classpath:/db"
                  :schema "default"
                  :datasource ds
                  :vendor (vendor-from-url url)}]
      (map->Database db-cfg))))

(defn migrations-loc [{:keys [db-resource-path schema vendor]}]
  {:pre [db-resource-path schema vendor]}
  (format "%s/%s/%s/migrations" db-resource-path (name vendor) schema))

(defn statements-loc [{:keys [db-resource-path schema vendor]}]
  {:pre [db-resource-path vendor schema]}
  (format "%s/%s/%s/statements" db-resource-path (name vendor) schema))

(defn add-migrator
  [db]
  {:pre [(:datasource db)]}
  (let [fw (doto
            (Flyway.)
             (.setDataSource (:datasource db))
             (.setLocations (into-array String [(migrations-loc db)])))
        fw (if (= (:schema db) "default")
             fw
             (doto fw
               (.setSchemas (into-array String [(:schema db)]))))]
    (assoc db :migrator fw)))

(defn close [{:keys [datasource]}]
  {:pre [datasource]}
  (.close datasource))

(defn info
  [{:keys [migrator]}]
  {:pre [migrator]}
  (.info migrator))

(defn version
  [db]
  (when-let [mig-info (-> db
                          (info)
                          (.current))]
    (-> mig-info
        (.getVersion)
        (.getVersion))))

(defn migrate
  [{:keys [migrator] :as db}]
  {:pre [migrator]}
  (.migrate migrator)
  (version db))

(defn clean
  [{:keys [migrator]}]
  (.clean migrator))