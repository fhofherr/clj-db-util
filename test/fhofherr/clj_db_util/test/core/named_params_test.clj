(ns fhofherr.clj-db-util.test.core.named-params-test
  (:require [clojure.test :refer :all]
            [fhofherr.clj-db-util.core.named-params :as named-params]))

(deftest parse-sql-str

  #_(testing "return named parameters in the order of occurence"
    (is (= {:sql-str "?" :params  [:param1]}
           (named-params/parse-sql-str ":param1")))
    (is (= {:sql-str "? ?" :params [:param1 :param2]}
           (named-params/parse-sql-str ":param1 :param2")))
    (is (= {:sql-str "? ?" :params [:param1 :param2]}
           (named-params/parse-sql-str ":param1,:param2"))))

  #_(testing "parameters are treated independent of each other"
    (is (= {:sql-str "? ?" :params [:param :param]}
           (named-params/parse-sql-str ":param :param"))))

  #_(testing "consecutive parameters are treated as one"
    (is (= {:sql-str "? ?" :params [:param1:param2]}
           (named-params/parse-sql-str ":param1:param2"))))

  #_(testing "parameters in quotes are ignored"
    (is (= {:sql-str "? ':ignored'" :params [:param]}
           (named-params/parse-sql-str ":param ':ignored'")))
    (is (= {:sql-str "? ':ignored'" :params [:param]}
           (named-params/parse-sql-str ":param '':ignored''")))
    (is (= {:sql-str "\":ignored\" ?" :params [:param]}
           (named-params/parse-sql-str "\":ignored\" :param")))
    (is (= {:sql-str "\":ignored\" ?" :params [:param]}
           (named-params/parse-sql-str "\"\":ignored\"\" :param"))))

  (testing "all other text is returned as-is"
    (is (= {:sql-str "SELECT * FROM t_key_value_pairs" :params []}
           (named-params/parse-sql-str "SELECT * FROM t_key_value_pairs")))))

(defn- input-consumed?
  [{:keys [input]}]
  (nil? input))

(defn- input=
  [expected {:keys [input]}]
  (= (seq expected) input))

(defn- accept-fn=
  [expected {:keys [accept-fn]}]
  (= expected accept-fn))

(defn- final-parse?
  [parse]
  (if-not (and (input-consumed? parse)
               (accept-fn= nil parse))
    (do
      (printf "Input consumed? %s\n" (input-consumed? parse))
      (printf "accept-fn == nil? %s\n" (accept-fn= nil parse))
      false)
    true))

(defn- parse-result=
  [expected {:keys [parse-result]}]
  (= expected parse-result))

(deftest accept-any-token
  (testing "accept anything except whitespace"

    (let [next-parse (-> "not-white-space"
                         (named-params/init-parse)
                         (named-params/accept-any-token))]
      (is (final-parse? next-parse))
      (is (parse-result= [[:any-token "not-white-space"]] next-parse)))

    (let [next-parse (-> "first-token second-token"
                         (named-params/init-parse)
                         (named-params/accept-any-token))]
      (is (input= " second-token" next-parse))
      (is (accept-fn= named-params/accept-whitespace next-parse))
      (is (parse-result= [[:any-token "first-token"]] next-parse)))))

(deftest accept-whitespace
  (testing "accept only whitespace"

    (let [next-parse (-> " \r\n\t,"
                         (named-params/init-parse)
                         (named-params/accept-whitespace))]
      (is (final-parse? next-parse))
      (is (parse-result= [[:whitespace " \r\n\t,"]] next-parse)))

    (let [next-parse (-> " some-token"
                         (named-params/init-parse)
                         (named-params/accept-whitespace))]
      (is (input= "some-token" next-parse))
      (is (accept-fn= named-params/accept-any-token next-parse))
      (is (parse-result= [[:whitespace " "]] next-parse)))

    (let [next-parse (-> " :named-param"
                         (named-params/init-parse)
                         (named-params/accept-whitespace))]
      (is (input= ":named-param" next-parse))
      (is (accept-fn= named-params/accept-named-parameter next-parse))
      (is (parse-result= [[:whitespace " "]] next-parse)))))

(deftest accept-named-parameter
  (testing "accept a colon followed by at least one non-whitespace character"
    (let [next-parse (-> ":named-param"
                         (named-params/init-parse)
                         (named-params/accept-named-parameter))]
      (is (final-parse? next-parse))
      (is (parse-result= [[:named-param ":named-param"]] next-parse)))

    (let [next-parse (-> ":param1 :param2"
                         (named-params/init-parse)
                         (named-params/accept-named-parameter))]
      (is (input= " :param2" next-parse))
      (is (accept-fn= named-params/accept-whitespace next-parse))
      (is (parse-result= [[:named-param ":param1"]] next-parse)))))

(deftest apply-accept-fn

  (testing "return the parse result if the input is fully consumed"
    (let [result (-> "some-token"
                     (named-params/init-parse)
                     (assoc :accept-fn named-params/accept-any-token)
                     (named-params/apply-accept-fn))]
      (is (= [[:any-token "some-token"]] result))))

  (testing "return a function if there is some input left"
    (let [intermediate-result (-> "some-token\t"
                                  (named-params/init-parse)
                                  (assoc :accept-fn named-params/accept-any-token)
                                  (named-params/apply-accept-fn))
          final-result (intermediate-result)]
      (is (function? intermediate-result))
      (is (= [[:any-token "some-token"] [:whitespace "\t"]] final-result)))))