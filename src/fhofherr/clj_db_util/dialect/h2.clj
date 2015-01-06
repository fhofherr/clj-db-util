(ns fhofherr.clj-db-util.dialect.h2
  (:require [clojure.java.io :refer [resource]]
            [clojure.zip :as zip]
            [fhofherr.clj-db-util.dialect.ast :as ast]))

(defn get-generated-key
  [m]
  (get m (keyword "scope_identity()")))
