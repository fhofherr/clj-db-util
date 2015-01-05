(ns fhofherr.clj-db-util.transactions
  (:require [clojure.java.jdbc :as jdbc]))

(defrecord Transaction [op])

(defn tx-return [v] (Transaction. (fn [dialect con]
                                    {::connection con ::value v})))

(defn tx-bind [tx f]
  (Transaction. (fn [dialect con]
                  (let [{con* ::connection v ::value} ((:op tx) dialect con)
                        tx* (f v)]
                    ((:op tx*) dialect con*)))))

(defmacro deftxfn
  [fn-name & body]
  (let [has-doc? (string? (first body))
        doc-string (if has-doc? (first body) nil)
        [args & decls] (if has-doc? (rest body) body)]
    (assert (symbol? fn-name)
            "Need a symbol as function name!")
    (assert (<= 2 (count args))
            "The first two arguments are needed for dialect and db-spec!")
    `(defn ~fn-name {:doc ~doc-string}
       [~@(drop 2 args)]
       (Transaction. (fn [~@(take 2 args)]
                       {::connection ~(second args)
                        ::value (do ~@decls)})))))

(defmacro deftxfn-
  [fn-name & body]
  `(deftxfn ~(with-meta fn-name (assoc (meta fn-name) :private true))
     ~@body))

(defn tx-exec
  [dialect db-spec tx]
  (jdbc/with-db-transaction [con db-spec]
    (::value ((:op tx) dialect con))))
