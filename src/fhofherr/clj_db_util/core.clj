(ns fhofherr.clj-db-util.core
  "The `core` namespace provides acces to all of `clj-db-util`'s core functionality.
  Applications should usually not need to import any other of `clj-db-util`'s namespaces.

  The `core` namespace provides access to the following features:

  * Connect to a database using a connection pool ([[connect-to-db]]).
  * Manage database schema changes using [Flyway](https://flywaydb.org/) ([[db-info]],
    [[db-version]], [[migrate-db]], [[clean-db]]).
  * Manage database transaction using monadic wrappers around the functions
    provided by [`clojure.java.jdbc`](https://github.com/clojure/java.jdbc) ([[query]],
    [[insert!]], [[update!]], [[delete!]], [[execute!]], [[rollback!]], [[with-db-transaction]]).
    Arbitrary values can be lifted into database transactions using the [[transactional]]
    macro. New transactional operations are defined using the [[transactional-operation]] macro.
  * The wrappers arround database operations ([[query]], [[update!]], [[delete!]], [[execute!]])
    support named parameters in addition to positional parameters.
  * Put statements into resource files to avoid large statements as strings in your code ([[load-stmt]])."
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.tools.logging :as log]
            [clojure.java.io :as io]
            [fhofherr.clj-db-util.core.named-params :as named-params])
  (:import (javax.sql DataSource)
           (org.flywaydb.core Flyway)
           (com.zaxxer.hikari HikariDataSource HikariConfig)
           (org.flywaydb.core.api MigrationInfoService)
           (clojure.lang Keyword)))

(defrecord Database [^DataSource datasource
                     ^String db-resource-path
                     ^String schema
                     ^Keyword vendor
                     ^Flyway migrator])

(alter-meta! #'->Database assoc :no-doc true)
(alter-meta! #'map->Database assoc :no-doc true)

(defn database?
  "Test if `db` is a database in the sense of `clj-db-util`."
  {:added "0.2.0"}
  [db]
  (instance? Database db))

(defn migrations-loc
  "Obtain the classpath location of schema migrations for `db`."
  {:added    "0.2.0"
   :arglists '([db])}
  [{:keys [db-resource-path schema vendor]}]
  {:pre [db-resource-path schema vendor]}
  (format "%s/%s/%s/migrations" db-resource-path (name vendor) schema))

(defn statements-loc
  "Obtain the classpath location of statements to execute against `db`."
  {:added "0.2.0"
   :arglists '([db])}
  [{:keys [db-resource-path schema vendor]}]
  {:pre [db-resource-path vendor schema]}
  (format "%s/%s/%s/statements" db-resource-path (name vendor) schema))

(defn- vendor-from-url
  [^String url]
  (cond
    (.startsWith url "jdbc:h2") :h2
    (.startsWith url "jdbc:postgresql") :postgres
    :else (throw (ex-info (format "Can't determine vendor for database url: %s" url)
                          {:cause #{:unknown-database-vendor}
                           :url url}))))

(defn- add-migrator
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

(defn connect-to-db
  "Create a connection pool for the database identified by `url`, `user`, and `password`."
  {:added "0.2.0"}
  [^String url ^String user ^String password]
  {:pre [url user password]}
  (let [cfg (doto (HikariConfig.)
              (.setJdbcUrl url)
              (.setUsername user)
              (.setPassword password))
        ds (HikariDataSource. cfg)]
    (let [db-cfg {:db-resource-path "db"
                  :schema "default"
                  :datasource ds
                  :vendor (vendor-from-url url)}]
      (-> db-cfg
          (map->Database)
          (add-migrator)))))

(defn disconnect-from-db
  "Close the connection pool used to connect to the database."
  {:added "0.2.0"}
  [{:keys [^HikariDataSource datasource]}]
  {:pre [datasource]}
  (.close datasource))

(defmacro with-database
  "Execute the expressions in the `body` with a database bound to `db-sym`.
  Disconnect from the database afterwards. Evaluate to whatever `body` evaluates.

  Example:

  ```clojure
  (with-database [db (connect-to-db \"url\" \"user\" \"password\"]
    ;; your code here
    (println \"Hello World\"))
  ```"
  {:added "0.2.0"}
  [[db-sym connect-expr :as bnd] & body]
  {:pre [(symbol? db-sym) connect-expr]}
  `(let ~bnd
     (try
       ~@body
       (finally
         (disconnect-from-db ~db-sym)))))

(defn verify-connection
  "Verify that connections to `db` can be obtained."
  {:added "0.2.0"}
  [db]
  {:pre [db]}
  (let [c  (.getConnection ^DataSource (:datasource db))]
    (try
      (.isValid c 5)
      (finally
        (.close c)))))

(defn ^MigrationInfoService db-info
  "Retrieve all information about the database. This includes information
  about applied, pending and current migrations.

  See
  [Flyway's API documenetation](http://flywaydb.org/documentation/api/javadoc/org/flywaydb/core/Flyway.html#info--)
  for details."
  {:added "0.2.0"}
  [{:keys [^Flyway migrator]}]
  {:pre [migrator]}
  (.info migrator))

(defn db-version
  "Get the database's current version"
  {:added "0.2.0"}
  [db]
  (some-> db
          (db-info)
          (.current)
          (.getVersion)
          (.getVersion)))

(defn migrate-db
  "Migrate the database to the latest version.

  See
  [Flyway's API documenetation](http://flywaydb.org/documentation/api/javadoc/org/flywaydb/core/Flyway.html#migrate--)
  for details."
  {:added "0.2.0"}
  [{:keys [^Flyway migrator] :as db}]
  {:pre [migrator]}
  (.migrate migrator)
  (db-version db))

(defn clean-db
  "Wipe all database schemas managed by Flyway.

  **Attention**: This drops all tables in the affected database. Do not call from production code.

  See
  [Flyway's API documenetation](http://flywaydb.org/documentation/api/javadoc/org/flywaydb/core/Flyway.html#clean--)
  for details."
  {:added "0.2.0"}
  [{:keys [^Flyway migrator]}]
  (.clean migrator))

(defn- db-spec
  [{:keys [datasource]}]
  {:pre [datasource]}
  {:datasource datasource})

(defrecord TransactionState [t-con db])
(alter-meta! #'->TransactionState assoc :no-doc true)
(alter-meta! #'map->TransactionState assoc :no-doc true)

(defn transaction-state?
  "Test if the passed object represents an transaction state."
  {:added "0.2.0"}
  [tx-state]
  (instance? TransactionState tx-state))

(def err-transaction-rolled-back {:error "Transaction rolled back"})

(defn- rollback-only?
  [{:keys [t-con]}]
  {:pre [t-con]}
  (jdbc/db-is-rollback-only t-con))

(defn- set-rollback-only!
  [{:keys [t-con] :as tx-state}]
  {:pre [t-con (transaction-state? tx-state)]}
  (jdbc/db-set-rollback-only! t-con)
  tx-state)

(defn ^:dynamic *exception-during-transaction*
  "Handle exceptions that occur during a database transaction. The default implementation
  logs the occured exception and then rolls back the transaction. As this method is
  dynamic it may be rebound to another implementation. Implementors must then take care
  of rolling back the transaction if necessary."
  {:added "0.2.0"}
  [tx-state ^Exception ex]
  (log/warn ex "Exception occured during transaction")
  ;; TODO repeatedly call and log getNextException (if possible) (see java.sql.BatchUpdateException)
  (let [rolled-back (set-rollback-only! tx-state)]
    [nil rolled-back]))

(defmacro transactional-operation
  "Create function `tx-state -> [x tx-state*]` where `tx-state*` is the
  possibly altered transaction state and `x` is an arbitrary result. The `body` used
  to define the transactional operation must evaluate tp `[x tx-state*]`.

  In most cases it should not be necessary to call this macro directly."
  {:added "0.2.0"}
  [[tx-state-bnd] & body]
  {:pre [(symbol? tx-state-bnd) (not-empty body)]}
  ;; TODO add meta data identifying the function as transactional operation?
  `(fn [~tx-state-bnd]
     {:post [(sequential? ~'%) (transaction-state? (second ~'%))]}
     (let [tx-state# ~tx-state-bnd]
       (try
         (if (#'rollback-only? tx-state#)
           [nil tx-state#]
           (io! ~@body))
         (catch Exception ex#
           (*exception-during-transaction* tx-state# ex#))))))

(defmacro transactional
  "Wrap `form` into a transaction. `form` will be evaluated upon transaction execution."
  {:added "0.2.0"}
  [form]
  `(transactional-operation [tx-state#] [~form tx-state#]))

(defn transactional-bind
  "Combine two transactional operations. `f` is expected to be a function `x -> tx-op*`
  where `x` is the result of `tx-op`."
  {:added "0.2.0"}
  [tx-op f]
  (transactional-operation
   [tx-state]
   (let [[res next-tx-state] (tx-op tx-state)
         next-tx-op (f res)]
     (next-tx-op next-tx-state))))

(defmacro transactional-let
  "Evaluate `body` in a lexical context in which the symbols in `bindings*` are bound to the results
  of previous transactional operations."
  {:added "0.2.0"}
  [bindings* & body]
  {:pre [(not-empty bindings*) (even? (count bindings*)) (not-empty body)]}
  (let [emit-tx-op-factory (fn [bnd tx-op]
                             `(fn [~bnd] ~tx-op))
        emit-binding-op-factoy (fn [[tx-op op-factory] [bnd-sym prev-tx-op]]
                                 (let [prev-op-factory (emit-tx-op-factory bnd-sym
                                                                           `(transactional-bind ~tx-op ~op-factory))]
                                   [prev-tx-op prev-op-factory]))
        reversed-bindings (->> bindings*
                               (partition 2)
                               (reverse))
        [innermost-bnd-sym innermost-tx-op] (first reversed-bindings)
        innermost-op-factory (emit-tx-op-factory innermost-bnd-sym `(transactional ~@body))
        [outermost-tx-op outermost-op-factory] (reduce emit-binding-op-factoy
                                                       [innermost-tx-op innermost-op-factory]
                                                       (rest reversed-bindings))]
    `(transactional-bind ~outermost-tx-op ~outermost-op-factory)))

(defn- prepare-params
  [s param-vals]
  (if (map? param-vals)
    (let [{:keys [parsed-str param-keys]} (named-params/parse-str s)
          mapped-vals (map #(get param-vals %) param-keys)]
      (concat [parsed-str] mapped-vals))
    (concat [s] (seq param-vals))))

(defn- wrap-jdbc-fn
  [jdbc-fn & args]
  (transactional-operation
   [tx-state]
   (let [res (apply jdbc-fn (:t-con tx-state) args)]
     [res tx-state])))

(defn insert!
  "Insert one or more `records` into the `table`."
  {:added "0.2.0"}
  [table & records]
  {:pre [table (not-empty records)]}
  (apply wrap-jdbc-fn jdbc/insert! table records))

(defn update!
  "Update a value in a table."
  {:added "0.2.0"}
  ([table value]
   (update! table value nil))
  ([table value where-clause]
   (update! table value where-clause nil))
  ([table value where-clause param-vals]
   {:pre [table value]}
   (wrap-jdbc-fn jdbc/update! table value (prepare-params where-clause param-vals))))

(defn delete!
  "Delete a record from a table."
  {:added "0.2.0"}
  ([table]
   (delete! table nil))
  ([table where-clause]
   (delete! table where-clause nil))
  ([table where-clause param-vals]
   {:pre [table]}
   (wrap-jdbc-fn jdbc/delete! table (prepare-params where-clause param-vals))))

(defn query
  "Execute a query given as a string. The query may contain either positional (e.g. `?`) or
  named (e.g. `:param-name`) parameters."
  {:added "0.2.0"}
  ([sql-str]
   (query sql-str nil))
  ([sql-str param-vals]
   {:pre [sql-str]}
   (wrap-jdbc-fn jdbc/query (prepare-params sql-str param-vals))))

(defn execute!
  "Execute an arbitrary sql statement except for a query. The statement may contain either positional (e.g. `?`) or
  named (e.g. `:param-name`) parameters."
  {:added "0.2.0"}
  ([sql-str]
   (execute! sql-str nil))
  ([sql-str param-vals]
   {:pre [sql-str]}
   (wrap-jdbc-fn jdbc/execute! (prepare-params sql-str param-vals))))

(defn rollback!
  "Rollback the transaction. Further transactional operations will not be executed anymore."
  {:added "0.2.0"}
  []
  (transactional-operation
   [tx-state]
   [nil (set-rollback-only! tx-state)]))

(defn load-stmt
  "Load a sql statement from a resource. The statement may contain either positional or named parameters."
  {:added "0.2.0"}
  [stmt-name]
  (transactional-operation
    [tx-state]
    (let [stmt-path (str (statements-loc (:db tx-state)) "/" stmt-name)
          stmt-res (io/resource stmt-path)]
      (when-not stmt-res
        (throw (ex-info (format "Could not find statement resource '%s" stmt-path)
                        {:cause #{:statement-not-found}})))
      [(slurp stmt-res) tx-state])))

(defn with-db-transaction
  "Exectue `tx-op` within a database transaction."
  {:added "0.2.0"}
  [db tx-op]
  {:pre [(database? db)]}
  (io!
   (jdbc/with-db-transaction
     [t-con (db-spec db)]
     (let [tx-state (map->TransactionState {:t-con t-con :db db})
           [tx-result final-tx-state] (tx-op tx-state)]
       (if (#'rollback-only? final-tx-state)
         [tx-result err-transaction-rolled-back]
         [tx-result nil])))))