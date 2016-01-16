(ns fhofherr.clj-db-util.jdbc-template.ast
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

(defn iterate-ast
  "Eagerly iterate over the AST `ast` by repeatedly applying the function `f`.

  `f` is a function of one argument which is a 2-tuple of `[a l]`, where `a` is
  an arbitrary accumulator value, or `nil` if no accumulation is desired. The
  initial value for `a` has to be supplied to `iterate-ast` by the argument
  `acc`. The second part `l` of the tuple passed to `f` represents the current
  location within the AST. `f` must return a 2 or 3-tuple `[a* l*]`, or
  `[a* l* s]` respectively. `a*` is the modified accumulator `a`, or `nil` if no
  accumulation is desired. `l*` represents the next location in the AST.  `l*`
  must never be `nil`. If `s` is present in the tuple returned by `f` and if
  `s` equals `stop` `iterate-ast` terminates the iteration over the AST.
  Additionally `iterate-ast` terminates the iteration if `(clojure.zip/end?
  l*)` is `true`.

  After terminating the iteration `iterate-ast` returns a 2-tuple `[a** t]`.
  `a**` is the final accumulator as returned by the last application of `f`. `t`
  is the new AST as returned obtained from the location returned by the final
  application of `f`.

  *Parameters*:

  - `f` the function to repeatedly apply.
  - `acc` the initial accumulator value, or `nil` if no accumulation is desired.
  - `stop` value to signal the stop of the iteration. Defaults to `:end`."
  [f acc ast & [stop]]
  (letfn [(assert-loc-not-nil
            [loc]
            (assert (not (nil? loc))
                    "The location returned by f must not be nil!"))
          (not-reached-end
            [[_ loc end]]
            (assert-loc-not-nil loc)
            (and (not (zip/end? loc))
                 (not= (or stop :end) end)))
          (zip-up
            [[a loc _]]
            (assert-loc-not-nil loc)
            [a (zip/root loc)])]
    (->> [acc (zip ast)]
      (iterate f)
      (drop-while not-reached-end)
      (first)
      (zip-up))))

(defn rule?
  "Check if the current location within represents a rule of the grammar that
  led to the AST.

  A location within the AST is considered a rule if it is a branch node of the
  AST and if its first child is a keyword.

  *Parameters*:

  - `loc` a location within the AST."
  [loc]
  (boolean (and loc
                ;; clojure.zip uses meta information associated with loc.
                ;; If we reach the end of a depth first traversal loc does
                ;; not have any meta information associated with it. Calling
                ;; zip/branch? with the special end location will lead to a
                ;; NullPointerException.
                (not (zip/end? loc))
                (zip/branch? loc)
                (keyword? (first (zip/children loc))))))

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
    (first (zip/node loc))))

(defn rule=
  "Check if the location `loc` in the AST is a rule with name `rule-name`.

  Returns `true` if the rule names are equal, or `false` if the rule names
  don't match, `loc` is not a rule, or `loc` is `nil`.

  *Parameters*:

  - `rule-name` the expected rule name
  - `loc` a location within the AST."
  [rule-name loc]
  (if (rule? loc)
    (= rule-name (get-rule loc))
    false))

(defn ast-to-str
  "Convert the given `ast` to a string.

  *Parameters*:

  - `ast` the abstract syntax tree to convert to a string."
  [ast]
  (letfn [(add-str
            [[ss loc]]
            (if (string? (zip/node loc))
              [(conj ss (zip/node loc)) (zip/next loc)]
              [ss (zip/next loc)]))]
    (->> (iterate-ast add-str [] ast)
      (first)
      (filter (complement nil?))
      (clojure.string/join ""))))
