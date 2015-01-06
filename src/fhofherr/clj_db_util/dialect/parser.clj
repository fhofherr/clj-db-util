(ns fhofherr.clj-db-util.dialect.parser
  (:require [clojure.java.io :refer [resource]]
            [instaparse.core :as insta]))

(def ^:private parser (-> "fhofherr/clj-db-util/grammars/parser.bnf"
                          (resource)
                          (insta/parser)))

(defn parse
  [s & options]
  (apply insta/parse parser s options))

(defn parses
  [s & options]
  (apply insta/parses parser s options))
