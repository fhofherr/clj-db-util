(ns fhofherr.clj-db-util.core.named-params
  (:require [clojure.string :as string]))

(declare accept-whitespace)
(declare accept-any-token)

(defn whitespace?
  [c]
  (#{\space \newline \tab \return} c))

(defn- new-accept-fn
  [consume-fn dispatch-fn]
  (fn [{:keys [input] :as parse}]
    (let [[parse-result rest-input] (consume-fn input)
          next-accept-fn (dispatch-fn rest-input)]
      (-> parse
          (assoc :input (seq rest-input))
          (assoc :accept-fn next-accept-fn)
          (update :parse-result #(conj (vec %) (string/join "" parse-result)))))))

(def accept-whitespace (new-accept-fn #(split-with whitespace? %)
                                      #(when (seq %) accept-any-token)))

(def accept-any-token (new-accept-fn #(split-with (complement whitespace?) %)
                                     #(when (seq %) accept-whitespace)))

(def accept-named-parameter (new-accept-fn #(let [[pr ri] (split-with (complement whitespace?) (rest %))]
                                             [(cons \: pr) ri])
                                           #(when (seq %) accept-whitespace)))

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
    {:sql-str (string/join "" res)
     :params []}))