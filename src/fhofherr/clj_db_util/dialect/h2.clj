(ns fhofherr.clj-db-util.dialect.h2
  (:require [clojure.java.io :refer [resource]]
            [instaparse.core :as insta]))

(def h2-parser (-> "fhofherr/clj-db-util/grammars/h2.bnf"
                   (resource)
                   (insta/parser)))

(def replacements {:EQ "="
                   :PARAM "?"})
