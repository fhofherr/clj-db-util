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
            [clojure.string :as string]
            [fhofherr.clj-db-util.core.named-params :as named-params])
  (:import (javax.sql DataSource)
           (org.flywaydb.core Flyway)
           (com.zaxxer.hikari HikariDataSource HikariConfig)
           (org.flywaydb.core.api MigrationInfoService)
           (clojure.lang Keyword Named)
           (java.sql SQLException)))

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

(def ^:private default-db-opts {:db-resource-path "db"
                                :schema           "default"
                                :from-db-name     string/lower-case
                                :to-db-name       identity})

(defn connect-to-db
  "Create a connection pool for the database identified by `url`, `user`, and `password`.
  `db-opts` may contain one of the following keys to configure the database:

  * `:db-resource-path`: path to the database resources on the classpath
  * `:schema`: name of the schema managed by this database
  * `:from-db-name`: function that converts column and table names from their db
    internal representation to a presentation used by the program
  * `:to-db-name`: function that converts column and table names from their representation
    in the program to a db internal representation"
  {:added "0.2.0"}
  ([^String url ^String user ^String password]
   (connect-to-db url user password {}))
  ([^String url ^String user ^String password db-opts]
   {:pre [url user password (map? db-opts)]}
   (let [cfg (doto (HikariConfig.)
               (.setJdbcUrl url)
               (.setUsername user)
               (.setPassword password))
         ds (HikariDataSource. cfg)]
     (-> default-db-opts
         (merge db-opts)
         (assoc :datasource ds
                :vendor (vendor-from-url url))
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

(defrecord TransactionState [t-con db rollback-msg])
(alter-meta! #'->TransactionState assoc :no-doc true)
(alter-meta! #'map->TransactionState assoc :no-doc true)

(defn transaction-state?
  "Test if the passed object represents an transaction state."
  {:added "0.2.0"}
  [tx-state]
  (instance? TransactionState tx-state))

(defn- rollback-only?
  [{:keys [t-con]}]
  {:pre [t-con]}
  (jdbc/db-is-rollback-only t-con))

(defn- set-rollback-only!
  [{:keys [t-con] :as tx-state} rollback-cause]
  {:pre [t-con (transaction-state? tx-state)]}
  (let [cause (or rollback-cause "Transaction rolled back")]
    (log/infof "Set transaction to rollback only. Reason: %s" cause)
    (jdbc/db-set-rollback-only! t-con)
    (assoc tx-state :rollback-msg cause)))

(defn ^:dynamic *exception-during-transaction*
  "Handle exceptions that occur during a database transaction. The default implementation
  logs the occured exception and then rolls back the transaction. As this method is
  dynamic it may be rebound to another implementation. Implementors must then take care
  of rolling back the transaction if necessary."
  {:added "0.2.0"}
  [tx-state ^Exception ex]
  (log/warn ex "Exception occured during transaction")
  (loop [e ex]
    (when (instance? SQLException e)
      (log/warn e)
      (recur (.getNextException ^SQLException e))))
  (let [rolled-back (set-rollback-only! tx-state (.getMessage ex))]
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

(defmacro transactional-trace
  "Transactional trace level logging using print-style args.
  Wrapper around [clojure.tools.logging/trace](http://clojure.github.io/tools.logging/#clojure.tools.logging/trace)."
  {:added "0.2.0"}
  [& args]
  `(transactional (log/trace ~@args)))

(defmacro transactional-debug
  "Transactional debug level logging using print-style args.
  Wrapper around [clojure.tools.logging/debug](http://clojure.github.io/tools.logging/#clojure.tools.logging/debug)."
  {:added "0.2.0"}
  [& args]
  `(transactional (log/debug ~@args)))

(defmacro transactional-info
  "Transactional info level logging using print-style args.
  Wrapper around [clojure.tools.logging/info](http://clojure.github.io/tools.logging/#clojure.tools.logging/info)."
  {:added "0.2.0"}
  [& args]
  `(transactional (log/info ~@args)))

(defmacro transactional-warn
  "Transactional warn level logging using print-style args.
  Wrapper around [clojure.tools.logging/warn](http://clojure.github.io/tools.logging/#clojure.tools.logging/warn)."
  {:added "0.2.0"}
  [& args]
  `(transactional (log/warn ~@args)))

(defmacro transactional-error
  "Transactional error level logging using print-style args.
  Wrapper around [clojure.tools.logging/error](http://clojure.github.io/tools.logging/#clojure.tools.logging/error)."
  {:added "0.2.0"}
  [& args]
  `(transactional (log/error ~@args)))

(defmacro transactional-fatal
  "Transactional fatal level logging using print-style args.
  Wrapper around [clojure.tools.logging/fatal](http://clojure.github.io/tools.logging/#clojure.tools.logging/fatal)."
  {:added "0.2.0"}
  [& args]
  `(transactional (log/fatal ~@args)))

(defmacro transactional-tracef
  "Transactional trace level logging using format.
  Wrapper around [clojure.tools.logging/tracef](http://clojure.github.io/tools.logging/#clojure.tools.logging/tracef)."
  {:added "0.2.0"}
  [& args]
  `(transactional (log/tracef ~@args)))

(defmacro transactional-debugf
  "Transactional debug level logging using format.
  Wrapper around [clojure.tools.logging/debugf](http://clojure.github.io/tools.logging/#clojure.tools.logging/debugf)."
  {:added "0.2.0"}
  [& args]
  `(transactional (log/debugf ~@args)))

(defmacro transactional-infof
  "Transactional info level logging using format.
  Wrapper around [clojure.tools.logging/infof](http://clojure.github.io/tools.logging/#clojure.tools.logging/infof)."
  {:added "0.2.0"}
  [& args]
  `(transactional (log/infof ~@args)))

(defmacro transactional-warnf
  "Transactional warn level logging using format.
  Wrapper around [clojure.tools.logging/warnf](http://clojure.github.io/tools.logging/#clojure.tools.logging/warnf)."
  {:added "0.2.0"}
  [& args]
  `(transactional (log/warnf ~@args)))

(defmacro transactional-errorf
  "Transactional error level logging using format.
  Wrapper around [clojure.tools.logging/errorf](http://clojure.github.io/tools.logging/#clojure.tools.logging/errorf)."
  {:added "0.2.0"}
  [& args]
  `(transactional (log/errorf ~@args)))

(defmacro transactional-fatalf
  "Transactional fatal level logging using format.
  Wrapper around [clojure.tools.logging/fatalf](http://clojure.github.io/tools.logging/#clojure.tools.logging/fatalf)."
  {:added "0.2.0"}
  [& args]
  `(transactional (log/fatalf ~@args)))

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

(defn transactional-sequence
  "Combine a sequence of transactional operations into a single transactional operation collecting their results in a
  seq."
  {:added "0.2.0"}
  [tx-ops]
  (transactional-operation
   [tx-state]
   (letfn [(collect-ops [[results state] tx-op]
             (let [[res next-state] (tx-op state)]
               [(conj results res) next-state]))]
     (let [[final-result final-state] (reduce collect-ops
                                              [[] tx-state]
                                              tx-ops)]
       [(sequence final-result) final-state]))))

(defn- prepare-params
  [s param-vals]
  (if (map? param-vals)
    (let [{:keys [parsed-str param-keys]} (named-params/parse-str s)
          mapped-vals (map #(get param-vals %) param-keys)]
      (concat [parsed-str] mapped-vals))
    (concat [s] (seq param-vals))))

(defn- convert-name
  [direction x]
  {:pre [(#{:to-db-name :from-db-name} direction)]}
  (transactional-operation
   [tx-state]
   (let [restore-type (fn [orig-v v]
                        (cond
                          (symbol? orig-v) (symbol v)
                          (keyword? orig-v) (keyword v)
                          :else v))
         conv-f (get-in tx-state [:db direction])
         convert (fn [v] (if (and conv-f
                                  (or (string? v)
                                      (instance? Named v)))
                           (->> v
                                (name)
                                (conv-f)
                                (restore-type v))
                           v))
         res (cond
               (map? x) (->> x
                             (map (fn [[k v]] [(convert k) v]))
                             (into (empty x)))
               (sequential? x) (map convert x)
               :else (convert x))]
     [res tx-state])))

(defn convert-to-db-name
  "Convert `x` to its database name. If `x` is a string, keyword, or symbol directly convert it. If
  `x` is a map, convert the map's keys and return a map with the converted keys. If `x` is a sequential
  collection, convert the collection's values and return a seq with the converted values."
  [x]
  (convert-name :to-db-name x))

(defn convert-from-db-name
  "Convert `x` from its database name. If `x` is a string, keyword, or symbol directly convert it. If
  `x` is a map, convert the map's keys and return a map with the converted keys. If `x` is a sequential
  collection, convert the collection's values and return a seq with the converted values."
  [x]
  (convert-name :from-db-name x))

(defn- wrap-jdbc-fn
  [jdbc-fn & args]
  (transactional-operation
   [tx-state]
   (let [res (apply jdbc-fn
                    (:t-con tx-state)
                    args)]
     [res tx-state])))

(defn insert!
  "Insert one or more `records` into the `table`."
  {:added "0.2.0"}
  [table & records]
  {:pre [table (not-empty records)]}
  (transactional-let
   [_ (transactional-debugf "Inserting %s into table %s" records table)
    db-table (convert-to-db-name table)
    db-records (if (map? (first records))
                 (transactional-sequence (map convert-to-db-name records))
                 (transactional-sequence (into [(convert-to-db-name (first records))]
                                               (map #(transactional %) (rest records)))))
    db-res (apply wrap-jdbc-fn jdbc/insert! db-table db-records)
    res (transactional-sequence (map convert-from-db-name db-res))]
   res))

(defn update!
  "Update a value in a table."
  {:added "0.2.0"}
  ([table value]
   (update! table value nil))
  ([table value where-clause]
   (update! table value where-clause nil))
  ([table value where-clause param-vals]
   {:pre [table value]}
   (transactional-let
    [_ (transactional-debugf "Updating %s with %s where %s %s" table value where-clause param-vals)
     db-table (convert-to-db-name table)
     db-value (convert-to-db-name value)
     db-res (wrap-jdbc-fn jdbc/update! db-table db-value (prepare-params where-clause param-vals))
     res (transactional-sequence (map convert-from-db-name db-res))]
    res)))

(defn delete!
  "Delete a record from a table."
  {:added "0.2.0"}
  ([table]
   (delete! table nil))
  ([table where-clause]
   (delete! table where-clause nil))
  ([table where-clause param-vals]
   {:pre [table]}
   (transactional-let
    [_ (transactional-debugf "Deleting from %s where %s %s" table where-clause param-vals)
     db-table (convert-to-db-name table)
     db-res (wrap-jdbc-fn jdbc/delete! db-table (prepare-params where-clause param-vals))
     res (transactional-sequence (map convert-from-db-name db-res))]
    res)))

(defn query
  "Execute a query given as a string. The query may contain either positional (e.g. `?`) or
  named (e.g. `:param-name`) parameters."
  {:added "0.2.0"}
  ([sql-str]
   (query sql-str nil))
  ([sql-str param-vals]
   {:pre [sql-str]}
   (transactional-let
    [_ (transactional-debugf "Querying %s %s" sql-str param-vals)
     db-res (wrap-jdbc-fn jdbc/query (prepare-params sql-str param-vals))
     res (transactional-sequence (map convert-from-db-name db-res))]
    res)))

(defn execute!
  "Execute an arbitrary sql statement except for a query. The statement may contain either positional (e.g. `?`) or
  named (e.g. `:param-name`) parameters."
  {:added "0.2.0"}
  ([sql-str]
   (execute! sql-str nil))
  ([sql-str param-vals]
   {:pre [sql-str]}
   (transactional-let
    [_ (transactional-debugf "Executing %s %s" sql-str param-vals)
     res (wrap-jdbc-fn jdbc/execute! (prepare-params sql-str param-vals))]
    res)))

(defn rollback!
  "Rollback the transaction. Further transactional operations will not be executed anymore.
  Optionally a `rollback-cause` may be given."
  {:added "0.2.0"}
  ([]
    (rollback! nil))
  ([rollback-cause]
    (transactional-operation
     [tx-state]
     [nil (set-rollback-only! tx-state rollback-cause)])))

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
  "Exectue `tx-op` within a database transaction. Returns `[result error]` where
  `result` is the result of the last evaluated transactional operation. If the transaction was rolled-back
  or an exception occured `error` is a dictionary of the form `{:error rollback-cause}` where rollback cause
  specifies the cause for the rollback. If an execption occured `rollback-cause` is the
  exception's message."
  {:added "0.2.0"}
  [db tx-op]
  {:pre [(database? db)]}
  (io!
   (jdbc/with-db-transaction
     [t-con (db-spec db)]
     (let [tx-state (map->TransactionState {:t-con t-con :db db})
           [tx-result final-tx-state] (tx-op tx-state)]
       (if (rollback-only? final-tx-state)
         [tx-result {:error (:rollback-msg final-tx-state)}]
         [tx-result nil])))))