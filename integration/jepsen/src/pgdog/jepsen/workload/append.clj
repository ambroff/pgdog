(ns pgdog.jepsen.workload.append
  "Elle list-append. Each key is a row in the `append` table; its value is a
  comma-separated list of integers. Transactions are sequences of
  [:r k nil] and [:append k v] micro-ops."
  (:require [clojure.string :as str]
            [jepsen [checker :as checker]
                    [client :as client]]
            [jepsen.tests.cycle.append :as append]
            [next.jdbc :as j]
            [pgdog.jepsen.client :as pc])
  (:import (java.sql Connection)))

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

(def neighbor-counter
  "Alternates clients between append clients and neighbors (--neighbors)."
  (atom 0))

(defn neighbor!
  "What a neighbor does instead of an append txn: another application on the
  same pool (same user and database) that sets its session to READ COMMITTED
  the way pgjdbc does, then runs a trivial query. It never touches the append
  table, so the op completes as :fail and Elle ignores it."
  [test node conn-atom op]
  (try
    (let [^Connection conn (or @conn-atom (reset! conn-atom (pc/open test node)))]
      (.setAutoCommit conn true)
      (.setTransactionIsolation conn Connection/TRANSACTION_READ_COMMITTED)
      (j/execute! conn ["SELECT 1"])
      (assoc op :type :fail, :error [:neighbor]))
    (catch Throwable e
      (pc/close! @conn-atom)
      (reset! conn-atom nil)
      (assoc op :type :fail, :error [:neighbor (pc/error-type e)]))))

(defrecord Client [node conn neighbor?]
  client/Client
  (open! [_ test node]
    (Client. node (atom nil)
             (and (:neighbors test) (odd? (swap! neighbor-counter inc)))))

  (setup! [_ test])

  (invoke! [_ test op]
    (if neighbor?
      (neighbor! test node conn op)
      (pc/run-txn! test node conn op
                   (fn [c] (mapv (partial mop! c) (:value op))))))

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
  (let [w (append/test {:key-count          10
                        :min-txn-length     1
                        :max-txn-length     4
                        :max-writes-per-key 32
                        :consistency-models [(consistency-model (:isolation opts))]})]
    (assoc w
           :client  (Client. nil nil false)
           :checker (checker/compose {:elle      (:checker w)
                                      :isolation (pc/isolation-checker)}))))
