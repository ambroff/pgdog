(ns pgdog.jepsen.workload.append
  "Elle list-append. Each key is a row in the `append` table; its value is a
  comma-separated list of integers. Transactions are sequences of
  [:r k nil] and [:append k v] micro-ops."
  (:require [clojure.string :as str]
            [jepsen.client :as client]
            [jepsen.tests.cycle.append :as append]
            [next.jdbc :as j]
            [pgdog.jepsen.client :as pc]))

(defn parse-list [s]
  (when s
    (mapv parse-long (str/split s #","))))

(defn mop!
  [conn [f k v :as mop]]
  (case f
    :r
    (let [row (j/execute-one! conn ["SELECT v FROM append WHERE k = ?" k])]
      [f k (parse-list (:append/v row))])

    :append
    (do (j/execute! conn
                    [(str "INSERT INTO append AS t (k, v) VALUES (?, ?) "
                          "ON CONFLICT (k) DO UPDATE SET v = t.v || ',' || excluded.v")
                     k (str v)])
        mop)))

(defrecord Client [node conn]
  client/Client
  (open! [_ test node]
    (Client. node (atom nil)))

  (setup! [_ test])

  (invoke! [_ test op]
    (pc/run-txn! test node conn op
                 (fn [c] (mapv (partial mop! c) (:value op)))))

  (teardown! [_ test])

  (close! [_ test]
    (pc/close! @conn)))

(def consistency-model
  "Strongest model Postgres claims at each isolation level."
  {:serializable    :serializable
   :repeatable-read :snapshot-isolation
   :read-committed  :read-committed})

(defn workload
  [opts]
  (assoc (append/test {:key-count          10
                       :min-txn-length     1
                       :max-txn-length     4
                       :max-writes-per-key 32
                       :consistency-models [(consistency-model (:isolation opts))]})
         :client (Client. nil nil)))
