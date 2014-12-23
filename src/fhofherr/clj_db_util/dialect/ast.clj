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

  - `loc` a location within the AST.
  "
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
    (first (zip/children loc))))

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

(defn find-rule
  "Find the location of the rule `rule-name` within the subtree represented
  by `loc`.

  Return the location or `nil` if no such location could be found.

  *Parameters*:

  - `rule-name` name of the rule to look for
  - `loc` the location from which to begin with the search"
  [rule-name loc]
  (let [l (->> loc
               (iterate zip/next)
               (drop-while #(and (not (rule= rule-name %))
                                 (not (zip/end? %))))
               (first))]
    (when (rule= rule-name l)
      l)))

(defn token?
  "Check if the current location within the AST is a token.

  A location is considered to be a token if it is a [[rule?]] and has no
  children except for the rule's name, or if it is a leaf node and not a
  keyword representing the name of a rule.

  *Examples*:

  - `[:SELECT]` is a token since it is a [[rule?]] and has no children except
     for its rule name.
  - `[:SELECT-EXPR \"*\"]` is not a token since it is a [[rule?]] but has more
     than one child.
  - `\"1\"` is a token since it is not a branch and not a keyword.
  - `[\"_NAME\"]` is not a token since it is a branch but no [[rule?]].

   *Parameters*:

  - `loc` a location within the AST."
  [loc]
  (or (and (rule? loc)
           (= 1 (count (zip/children loc))))
      (and (not (zip/branch? loc))
           ;; TODO: It is possible to obtain the rule names used by an
           ;; instaparse parser, e.g. (keys (:grammar parser)). This
           ;; might be useful here.
           (not (keyword? (zip/node loc))))))

(defn token-to-str
  "If the location `loc` of the AST is a [[token?]] return the token's string
  representation and `(clojure.zip/next loc)`. Return `[nil (clojure.zip/next
  loc)]` if `loc`  does not represent a [[token?]].

   *Parameters*:

  - `loc` a location within the AST."
  [loc]
  (if (token? loc)
    (if (rule? loc)
      (let [rule (get-rule loc)]
        [(name rule) (zip/next loc)])
      [(str (zip/node loc)) (zip/next loc)])
    [nil (zip/next loc)]))

(defn ast-to-str
  "Convert the given `ast` to as string using the given `formatters` for
  tokens whose canonical string representation is not desired.

  The values in `formatters` may either be strings or functions of one argument
  (a location within the ast) returning a string and the next location within
  the ast to process. This allows to ignore subtrees that have already been
  consumed by the current formatter. If the formatter is a string `ast-to-str`
  uses `clojure.zip/next` to go to the next location in the AST. A formatter may
  return `nil` for the next location. In this case the creation of the string
  is stoped, and the string created so far is returned.

  *Parameters*:

  - `ast` the abstract syntax tree to convert to a string.
  - `formatters` (optional) map of formatters to use for rules whose canonical
  string representation is not desired."
  [ast & [formatters]]
  (letfn [(add-str
            [ss [s l] cl]
            (if l
              [(conj ss s) l]
              [(conj ss s) cl :end]))
          (apply-formatter
            [[ss loc]]
            (if (rule? loc)
              (let [fmt (get formatters (get-rule loc) token-to-str)]
                (if (string? fmt)
                  (add-str ss [fmt (zip/next loc)] loc)
                  (add-str ss (fmt loc) loc)))
              (add-str ss (token-to-str loc) loc)))]
    (->> (iterate-ast apply-formatter [] ast)
      (first)
      (filter (complement nil?))
      (clojure.string/join " "))))
