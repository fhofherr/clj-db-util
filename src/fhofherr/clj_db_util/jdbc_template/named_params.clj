(ns fhofherr.clj-db-util.jdbc-template.named-params
  (:require [clojure.zip :as zip]
            [fhofherr.clj-db-util.dialect.ast :as ast]))

(defn named-param?
  "Check if given location within an AST represents a named parameter.

  The location within the AST has to be a rule as defined by [[ast/rule?]] and
  its rule name has to be `:NAMED-PARAM`.

  *Parameters*:

  - `loc` a location within an AST."
  [loc]
  (and (ast/rule? loc)
       (= :NAMED-PARAM (ast/get-rule loc))))

(defn param?
  "Check if given location within an AST represents a positional parameter.

  The location within the AST has to be a rule as defined by [[ast/rule?]] and
  its rule name has to be `:PARAM`.

  *Parameters*:

  - `loc` a location within an AST."
  [loc]
  (and (ast/rule? loc)
       (= :PARAM (ast/get-rule loc))))

(defn- do-extract
  [[ps loc]]
  (if (named-param? loc)
    (let [param-name (-> loc
                         (zip/children)
                         (second)
                         (clojure.string/lower-case)
                         (keyword))
          next-loc (zip/next (zip/replace loc [:PARAM]))]
      [(conj ps param-name) next-loc])
    [ps (zip/next loc)]))

(defn extract-named-params
  "Extract all named parameters from the given abstract sytnax tree.
  Additionally replace all occurences of named parameters by positional
  parameters.

  Return a tuple `[ps t]` where `ps` is a collection of all names of named
  parameters as found during a depth first traversal of the AST. The parameter
  names in `ps` are converted to lower-cased keywords. `t` is the mofified AST
  containing positional parameters for all named parameters.

  See [[named-param?]] for the definition of named parameters and
  [[param?]] for the definition of positional parameters.

  *Parameters*:

  - `tree` the AST to process."
  [tree]
  (ast/iterate-ast do-extract [] tree))

(defn make-argv
  "Create a lazy sequence of values to substitute for the parameter names given
  in `ps`.

  Uses the map `m` to lookup the values. The returned lazy seq contains the
  values in the order defined by the order of the parameter names in `ps`.

  *Parameters*:

  - `ps` a sequential collection of parameter names.
  - `m` a map mapping the parameter names to their desired values."
  [ps m]
  (for [p ps] (get m p)))

(defn process-named-params
  "Process the named paramters within the AST `tree` and use the context `ctx`
  to resolve their respective values.

  Return a two-tuple `[argv tree]` where `argv` is an argument vector siutable
  for the respective `clojure.java.jdbc` functions. `tree` is the modified AST
  with all named parameters replaced by positional parameters.

  *Parameters*:

  - `ctx` context used to resolve the values of the named parameters.
  - `tree` AST to modify"
  [ctx tree]
  (let [[ps tree*] (extract-named-params tree)
        argv (make-argv ps ctx)]
    [argv tree*]))
