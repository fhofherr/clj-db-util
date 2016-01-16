(ns fhofherr.clj-db-util.jdbc-template.parser
  (:require [clojure.java.io :refer [resource]]
            [clojure.tools.logging :as log]
            [instaparse.core :as insta]))

(def ^:private parser (-> "fhofherr/clj-db-util/grammars/parser.bnf"
                          (resource)
                          (insta/parser)))
(defn parse
  [s & options]
  (let [ast (apply insta/parse parser s options)]
    (if (insta/failure? ast)
      (log/fatal (insta/get-failure ast))
      ast)))

(defn parses
  [s & options]
  (let [asts (apply insta/parses parser s options)]
    (if (insta/failure? asts)
      (log/fatal (insta/get-failure asts))
      asts)))
