(ns fhofherr.clj-db-util.jdbc-template.template-vars
  (:require [clojure.zip :as zip]
            [clojure.tools.logging :as log]
            [fhofherr.clj-db-util.jdbc-template [parser :as parser]
                                                [ast :as ast]]))

(defn- variable-not-found
  [errors var-name loc]
  [(conj errors
         (format "Could not find value for template variable %s" var-name))
   (zip/next loc)])

(defn- variable-had-parse-errors
  [errors var-name loc]
  [(conj errors (format "Variable '%s' contained parse errors!" var-name))
   (zip/next loc)])

(defn- replace-template-var
  [loc value]
  (let [parsed (parser/parse (str value) :start :SQL_TOKEN)]
    (when parsed
      (zip/next (zip/replace loc parsed)))))

(defn- eval-template-var
  [errors ctx loc]
  (let [[_ var-name] (zip/node loc)
        value (->> var-name
                   (clojure.string/lower-case)
                   (keyword)
                   (get ctx))]
    (if value
      (if-let [loc* (replace-template-var loc value)]
        [errors loc*]
        (variable-had-parse-errors errors var-name loc))
      (variable-not-found errors var-name loc))))

(defn process-template-vars
  "Replace any template variables within the AST `tree` by their values
  found in `ctx`. Validate the syntactical correctnes of the replacemtent
  by parsing it.

  *Parameters*:

  - `ctx` context containing the replacements.
  - `tree` AST containing template variables."
  [ctx tree]
  (letfn [(do-process [[errors loc]]
            (if (ast/rule= :TEMPLATE_VAR loc)
              (eval-template-var errors ctx loc)
              [errors (zip/next loc)]))]
    (let [[errors tree*] (ast/iterate-ast do-process [] tree)]
      (if (empty? errors)
        tree*
        (log/error (clojure.string/join "\n" errors))))))
