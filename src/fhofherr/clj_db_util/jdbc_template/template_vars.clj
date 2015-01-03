(ns fhofherr.clj-db-util.jdbc-template.template-vars
  (:require [clojure.zip :as zip]
            [clojure.tools.logging :as log]
            [fhofherr.clj-db-util.dialect :as d]
            [fhofherr.clj-db-util.dialect.ast :as ast]))

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
  [dialect parent value]
  (let [rule (ast/get-rule parent)
        parsed (d/parse dialect value :start rule)]
    (when parsed
      (zip/next (zip/replace parent parsed)))))

(defn- eval-template-var
  [dialect errors ctx loc]
  (let [[_ var-name] (zip/node (ast/find-rule :TEMPLATE-VAR-NAME loc))
        value (->> var-name
                   (clojure.string/lower-case)
                   (keyword)
                   (get ctx))]
    (if value
      (if-let [loc* (replace-template-var dialect (zip/up loc) value)]
        [errors loc*]
        (variable-had-parse-errors errors var-name loc))
      (variable-not-found errors var-name loc))))

(defn process-template-vars
  "Replace any template variables within the AST `tree` by their values
  found in `ctx`. Validate the syntactical correctnes of the replacemtent
  by parsing it using the `dialects` parser.

  *Parameters*:

  - `dialect` the dialect used to parse the replacement.
  - `ctx` context containing the replacements.
  - `tree` AST containing template variables."
  [dialect ctx tree]
  (letfn [(do-process [[errors loc]]
            (if (ast/rule= :TEMPLATE-VAR loc)
              (eval-template-var dialect errors ctx loc)
              [errors (zip/next loc)]))]
    (let [[errors tree*] (ast/iterate-ast do-process [] tree)]
      (if (empty? errors)
        tree*
        (log/error (clojure.string/join "\n" errors))))))
