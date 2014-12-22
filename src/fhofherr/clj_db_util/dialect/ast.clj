(ns fhofherr.clj-db-util.dialect.ast
  (:require [clojure.zip :as zip]))

(defn zip
  "Create a zipper from an abstract syntax tree (AST).

  The zipper returned by `zip` can be processed with the functions provided by
  [clojure.zip](http://clojure.github.io/clojure/clojure.zip-api.html)

  *Parameters*:

  - `root` the root of the AST
  "
  [root]
  (zip/vector-zip root))

(defn rule?
  "Check if the current location within represents a rule of the grammar that
  led to the AST.

  A location within the AST is considered a rule if it is a branch node of the
  AST and if its first child is a keyword.

  *Parameters*:

  - `loc` a location within the AST.
  "
  [loc]
  (and (zip/branch? loc)
       (keyword? (first (zip/children loc)))))

(defn get-rule
  "Obtain the name of the rule that lead to the branch of the AST at the
  current location.

  The location given to `get-rule` has to be a rule as defined by [[rule?]].
  The keyword which is the first child of a rule location is interpreted as the
  rules name and returned. If the given location does not represent a rule `nil`
  is returned.

  *Parameters*:

  - `loc` a location within the AST.
  "
  [loc]
  (when (rule? loc)
    (first (zip/children loc))))

(defn token?
  "Check if the current location within the AST is a token.

  A location is considered to be a token if it is a [[rule?]] and has no
  children except for the rule's name, or if it is a leaf node.

  *Examples*:

  - `[:SELECT]` is a token since it is a [[rule?]] and has no children except
     for its rule name.
  - `[:SELECT-EXPR \"*\"]` is not a token since it is a [[rule?]] but has more
     than one child.
  - `\"1\"` is a token since it is not a branch.
  - `[\"_NAME\"]` is not a token since it is a branch but no [[rule?]].

   *Parameters*:

  - `loc` a location within the AST."
  [loc]
  (or (and (rule? loc)
           (= 1 (count (zip/children loc))))
      (not (zip/branch? loc))))
