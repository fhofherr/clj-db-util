(ns fhofherr.clj-db-util.core.named-params
  (:require [clojure.string :as string]))

(declare accept-whitespace)
(declare accept-any-token)
(declare accept-named-parameter)

(defn whitespace?
  [c]
  (#{\space \newline \tab \return \,} c))

(defn- token
  [tok-kw tok-v]
  [tok-kw (string/join "" tok-v)])

(defn- accept
  [tok-kw consume-fn dispatch-fn]
  (fn [{:keys [input] :as parse}]
    (let [[parse-result rest-input] (consume-fn input)
          next-accept-fn (dispatch-fn rest-input)]
      (-> parse
          (assoc :input (seq rest-input))
          (assoc :accept-fn next-accept-fn)
          (update :parse-result #(conj (vec %)
                                       (token tok-kw parse-result)))))))

(def accept-whitespace (accept :whitespace
                               #(split-with whitespace? %)
                               #(when-let [rest-in (seq %)]
                                 (case (first rest-in)
                                   \: accept-named-parameter
                                   accept-any-token))))

(def accept-any-token (accept :any-token
                              #(split-with (complement whitespace?) %)
                              #(when (seq %) accept-whitespace)))

(def accept-named-parameter (letfn [(consume-named-param [input]
                                      (let [[result rest-in] (->> input
                                                                  (rest)
                                                                  (split-with (complement whitespace?)))]
                                        [(cons \: result) rest-in]))]
                              (accept :named-param
                                      consume-named-param
                                      #(when (seq %) accept-whitespace))))

(defn apply-accept-fn
  [{:keys [accept-fn] :as parse}]
  {:pre [accept-fn]}
  (let [next-parse (accept-fn parse)]
    (if (:input next-parse)
      #(apply-accept-fn next-parse)
      (:parse-result next-parse))))

(defn init-parse
  [sql-str]
  {:input (seq sql-str)})

(defn parse-sql-str
  [sql-str]
  (let [res (trampoline apply-accept-fn (-> sql-str
                                            (init-parse)
                                            (assoc :accept-fn accept-any-token)))]
    {:sql-str (string/join "" (map second res))
     :params  []}))