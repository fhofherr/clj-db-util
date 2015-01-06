(ns fhofherr.clj-db-util.dialect
  (:refer-clojure :exclude [name])
  (:require [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [instaparse.core :as insta]
            [fhofherr.clj-db-util.dialect.h2 :as h2-dialect]))

(def h2 {::name :h2
         ::resource-path "db/h2"
         ::gen-key-extractor h2-dialect/get-generated-key})

(defn set-resource-path
  [dialect resource-path]
  (assoc dialect ::resource-path resource-path))

(defn statements-loc
  "Get the path where the dialect expects its statement resources.

  *Parameters*:

  - `dialect` the SQL dialect to use"
  [dialect]
  (str (::resource-path dialect) "/statements/"))

(defn migrations-loc
  "Get the path where the dialect expects its migration resources.

  *Parameters*:

  - `dialect` the SQL dialect to use
  - `schema` (optional) if migrations for a database are seperated by schema."
  ([dialect]
   (migrations-loc dialect nil))
  ([dialect schema]
   (if schema
     (format "%s/migrations/%s/" (::resource-path dialect) schema)
     (format "%s/migrations/default/" (::resource-path dialect)))))

(defn load-statement
  "Load a SQL statement from a resource.

  The `stmt-path` is interpreted as a sub-path of the dialect's
  [[statements-loc]]. Returns the statement as a string or `nil` if
  no statement could be found.

  *Parameters*:

  - `dialect` the SQL dialect to use
  - `stmt-path` sub-path of the dialects [[statements-loc]]"
  [dialect stmt-path]
  (let [p (str (statements-loc dialect) stmt-path)
        r (io/resource p)]
    (if r
      (slurp r)
      (log/fatalf "Could not load statement '%s'" p))))

(defn get-generated-keys
  "Given a sequence `ms` of maps try to extract the generated keys
  using the `dialect`'s generated key extractor."
  [dialect ms]
  (map (::gen-key-extractor dialect) ms))
