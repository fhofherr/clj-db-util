(ns fhofherr.clj-db-util.core.named-params
  (:require [clojure.string :as string]))

(declare accept-whitespace)
(declare accept-any-token)
(declare accept-named-parameter)
(declare accept-quoted-string)

(defn whitespace?
  [c]
  (#{\space \newline \tab \return \,} c))

(defn named-param-start?
  [c]
  (= c \:))

(defn quote-char?
  [c]
  (#{\" \'} c))

(defn dispatch
  [c]
  (cond
    (named-param-start? c) accept-named-parameter
    (quote-char? c) accept-quoted-string
    (whitespace? c) accept-whitespace
    :else accept-any-token))

(defn- token
  [tok-kw tok-v]
  [tok-kw (string/join "" tok-v)])

(defn- accept
  [tok-kw consume-fn]
  (fn [{:keys [input] :as parse}]
    (let [[parse-result rest-input] (consume-fn input)
          next-accept-fn (when-let [[c & _] (seq rest-input)]
                           (dispatch c))]
      (-> parse
          (assoc :input (seq rest-input)
                 :accept-fn next-accept-fn)
          (update :parse-result #(if (seq parse-result)
                                   (conj (vec %) (token tok-kw parse-result))
                                   %))))))

(def accept-start (accept :start (fn [input] [nil input])))

(def accept-whitespace (accept :whitespace #(split-with whitespace? %)))

(def accept-any-token (accept :any-token #(split-with (complement whitespace?) %)))

(def accept-named-parameter (letfn [(consume-named-param [input]
                                      (let [[result rest-in] (->> input
                                                                  (rest)
                                                                  (split-with (complement whitespace?)))]
                                        [result rest-in]))]
                              (accept :named-param consume-named-param)))

(def accept-quoted-string (letfn [(consume-quoted [quote-char result [x y & rest-input]]
                                    (cond
                                      (nil? y) [(conj result x) rest-input]
                                      (and (= x quote-char) (not= y quote-char)) [(conj result x) (conj rest-input y)]
                                      (= x y quote-char) (recur quote-char (conj result x y) rest-input)
                                      :else (recur quote-char (conj result x) (conj rest-input y))))]
                            (accept :quoted-string #(consume-quoted (first %) [(first %)] (rest %)))))

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
  (let [parse-result (trampoline apply-accept-fn (-> sql-str
                                                     (init-parse)
                                                     (assoc :accept-fn accept-start)))
        [sql-str params] (reduce (fn [[ss ps] [tok-kw tok-v]]
                                   (if (= :named-param tok-kw)
                                     [(conj ss "?") (conj ps (keyword tok-v))]
                                     [(conj ss tok-v) ps]))
                                 [[] []]
                                 parse-result)]
    {:sql-str (string/join "" sql-str)
     :params  params}))