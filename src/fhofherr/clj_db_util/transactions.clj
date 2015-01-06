(ns fhofherr.clj-db-util.transactions
  (:require [clojure.java.jdbc :as jdbc]
            [fhofherr.clj-db-util.db :as db-repr]))

;; Wrapper for a transaction. `op` is a function of one argument returning
;; a map {::connection con ::value v}. The argument to `op` is the database
;; currently in use.
(defrecord Transaction [op])

(alter-meta! #'->Transaction assoc :no-doc true)
(alter-meta! #'map->Transaction assoc :no-doc true)

(defmacro deftx
  "Define a function that creates a transaction. The first entry of the
  definitions argument vector will be bound to the database used during the
  execution of the transaction. Further arguments are optional."
  {:arglists '([fn-name [db & args] & body]
               [fn-name doc-string [db & args] & body])}
  [fn-name & body]
  (let [has-doc? (string? (first body))
        doc-string (if has-doc? (first body) nil)
        [args & decls] (if has-doc? (rest body) body)]
    (assert (symbol? fn-name) "Need a symbol as function name!")
    (assert (<= 1 (count args))
            "The first arguments is needed for the database!")
    `(defn ~fn-name {:doc ~doc-string}
       [~@(rest args)]
       (Transaction. (fn [~(first args)]
                       {::connection ~(first args)
                        ::value (do ~@(seq decls))})))))

(defmacro deftx-
  "Same as [[deftx]] but yielding a non-public def."
  {:arglists '([fn-name [db & args] & body]
               [fn-name doc-string [db & args] & body])}
  [fn-name & body]
  `(deftx ~(with-meta fn-name (assoc (meta fn-name) :private true))
     ~@body))

(deftx tx-apply
  "Take the function `f` and apply it to the values `v` and `vs` within
  the context of a transaction."
  [db f v & vs]
  (apply f v vs))

(deftx tx-return
  "Take an arbitrary value `v` and wrap it into a transaction.

  *Parameters*:

  - `v` an arbitrary value to wrap into a transaction."
  [db v]
  v)

;; Can't use deftx here as we need to return con* for the ::connection.
;; If we would use deftx con would be returned.
(defn tx-bind
  "Take a transacton `tx` containting a value `v` and a function `f` of
  one argument returning another transaction. Apply `f` to `v` and return
  the resulting transaction.

  *Parameters*:

  - `tx` a transaction.
  - `f` a function expecting one argument and returning a transaction."
  [tx f]
  (Transaction. (fn [db]
                  (if-not (jdbc/db-is-rollback-only (db-repr/db-spec db))
                    (let [{db* ::connection v ::value} ((:op tx) db)
                          tx* (f v)]
                      (if-not (jdbc/db-is-rollback-only (db-repr/db-spec db*))
                        ((:op tx*) db*)
                        {::connection db* ::value nil}))
                    {::connection db ::value nil}))))

(deftx tx-rollback
  "Rollback the transaction. Further steps won't be executed."
  [db]
  (jdbc/db-set-rollback-only! (db-repr/db-spec db)))

(defn- emit-tx-step
  [[arg tx-expr] last-exprs]
  (if tx-expr
    `(tx-bind (fn [~arg] ~tx-expr))
    (if (seq last-exprs)
      `(tx-bind (fn [~arg] (tx-return (do ~@last-exprs))))
      `(tx-bind (fn [~arg] (tx-return ~arg))))))

(defn- emit-tx-steps
  [bindings last-exprs]
  (reduce (fn [a s] (conj a (emit-tx-step s last-exprs))) [] bindings))

(defmacro tx->
  "Evaluate transactions within a lexical context. The values of the
  transactions are bound to the given binding symbols. The last transaction
  in the bindings is returned as the evaluation result.

  *Examples*:

  ```clojure
  (tx-> _ (t/insert! :fruit {:name \"Apple\" :cost 2.99})
        _ (t/insert! :fruit_orders
                     {:fruit_id 1 :customer_name \"Fruit Sales Inc.\"})
  ```"
  [bindings & last-exprs]
  (assert (even? (count bindings))
          "Need an even number of bindings!")
  (let [args (->> bindings
               (partition 2)
               (map first))
        exprs (->> bindings
                (partition 2)
                (map second))
        first-tx-expr (first exprs)
        other-tx-exprs (-> exprs
                           (rest)
                           (lazy-cat (repeat nil)))
        reordered-bindings (partition 2 (interleave args other-tx-exprs))]
    `(-> ~first-tx-expr
         ~@(emit-tx-steps reordered-bindings last-exprs))))

(defn tx-exec
  "Execute the transaction `tx` in the database represented by `db-spec`.
  The `dialect` is added for convenience since some database functions need it
  to perform their tasks (this might not be a good idea, in which case I'll
  remove the dialect)."
  [db tx]
  (jdbc/with-db-transaction [db* (db-repr/db-spec db)]
    (::value ((:op tx) (db-repr/from-db-spec (db-repr/dialect db) db*)))))

(defmacro tx-exec->
  "Like [[tx->]] but immediately call [[tx-exec]] on the result."
  [db bindings & last-exprs]
  `(tx-exec ~db (tx-> ~bindings ~@last-exprs)))
