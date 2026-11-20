(ns pgdog.jepsen.workload.append
  "Elle list-append. Each key is a row in one of the append tables (see
  db/append-tables; --tables N spreads keys over N tables by hash of the
  key); its value is a comma-separated list of integers. Transactions are
  sequences of [:r k nil] and [:append k v] micro-ops.

  Following Jepsen's PostgreSQL 12.3 test, each micro-op can take one of
  several SQL paths, so different statements go through PgDog's parser and
  router:

    --append-path upsert         INSERT ... ON CONFLICT (k) DO UPDATE
    --append-path update-insert  UPDATE; if no row, INSERT inside a SAVEPOINT;
                                 on a unique violation ROLLBACK TO SAVEPOINT and
                                 UPDATE again
    --append-path mixed          either, per micro-op
    --read-by pk | sk | mixed    read by primary key, or by the unindexed
                                 secondary key `sk` (a table scan), or either"
  (:require [clojure.string :as str]
            [jepsen [checker :as checker]
                    [client :as client]
                    [history :as h]]
            [jepsen.generator :as gen]
            [jepsen.random :as rand]
            [jepsen.tests.cycle.append :as append]
            [next.jdbc :as j]
            [pgdog.jepsen.client :as pc]
            [pgdog.jepsen.db :as db])
  (:import (java.sql Connection)))

(defn parse-list [s]
  (when s
    (mapv parse-long (str/split s #","))))

(defn table
  "The table key k lives in."
  [test k]
  (let [ts (db/append-tables test)]
    (nth ts (mod (hash k) (count ts)))))

(defn pick
  "Resolves a :mixed option to one of `choices` for a single micro-op."
  [x choices]
  (if (= :mixed x) (rand/nth choices) x))

(defn update-count [result]
  (:next.jdbc/update-count (first result)))

(defn append-upsert! [conn t k v]
  (j/execute! conn
              [(str "INSERT INTO " t " AS t (k, sk, v) VALUES (?, ?, ?) "
                    "ON CONFLICT (k) DO UPDATE SET v = t.v || ',' || excluded.v")
               k k (str v)]))

(defn append-update-insert!
  "Jepsen's second append path: UPDATE, check the row count, INSERT if the
  row doesn't exist, and if a concurrent transaction inserted it first (unique
  violation), roll back to a savepoint and UPDATE again. The savepoint keeps
  the transaction usable after the INSERT error."
  [conn t k v]
  (let [update! #(update-count
                   (j/execute! conn [(str "UPDATE " t " SET v = v || ',' || ? WHERE k = ?")
                                     (str v) k]))]
    (when (zero? (update!))
      (j/execute! conn ["SAVEPOINT upsert"])
      (try
        (j/execute! conn [(str "INSERT INTO " t " (k, sk, v) VALUES (?, ?, ?)") k k (str v)])
        (j/execute! conn ["RELEASE SAVEPOINT upsert"])
        (catch Exception e
          (if (= "23505" (pc/sql-state e))
            (do (j/execute! conn ["ROLLBACK TO SAVEPOINT upsert"])
                ;; At RR/SER the row may be invisible to our snapshot; then
                ;; this txn can't append and must abort.
                (when (zero? (update!))
                  (throw (ex-info "row inserted concurrently but not visible"
                                  {:type :invisible-conflict, :k k}))))
            (throw e)))))))

(defn mop!
  [test conn [f k v :as mop]]
  (let [t (table test k)]
    (case f
      :r
      (let [col (name (pick (:read-by test) [:pk :sk]))
            col (if (= "pk" col) "k" "sk")
            row (j/execute-one! conn [(str "SELECT v FROM " t " WHERE " col " = ?") k])]
        [f k (parse-list (some-> row vals first))])

      :append
      (do (case (pick (:append-path test) [:upsert :update-insert])
            :upsert        (append-upsert! conn t k v)
            :update-insert (append-update-insert! conn t k v))
          mop))))

(defrecord FinalReads []
  ;; Like jepsen.tests.cycle.core/final-gen, which reads
  ;; (partition 8 (range max-key)) and so skips the max key and the last
  ;; partial group of 8. Reads every key 0..max-key, marked :final? so
  ;; neighbor clients run them too.
  gen/Generator
  (op [_ test ctx]
    (when-let [mk (:max-key ctx)]
      (gen/op (->> (range (inc mk))
                   (partition-all 8)
                   (map (fn [ks]
                          {:f :txn, :final? true, :value (mapv (fn [k] [:r k nil]) ks)})))
              test ctx)))
  (update [this _ _ _] this))

(def neighbor-counter
  "Alternates clients between append clients and neighbors (--neighbors)."
  (atom 0))

(defn neighbor!
  "What a neighbor does instead of an append txn: another application on the
  same pool (same user and database) that sets its session to READ COMMITTED
  the way pgjdbc does, then runs a trivial query. It never touches the append
  tables, so the op completes as :fail and Elle ignores it."
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
    (if (and neighbor? (not (:final? op)))
      (neighbor! test node conn op)
      (pc/run-txn! test node conn op
                   (fn [c] (mapv (partial mop! test c) (:value op))))))

  (teardown! [_ test])

  (close! [_ test]
    (pc/close! @conn)))

(def consistency-model
  "Strongest model Postgres claims at each isolation level, for any topology."
  {:serializable    :serializable
   :repeatable-read :snapshot-isolation
   :read-committed  :read-committed})

(def strong-consistency-model
  "With a single Postgres behind PgDog: Jepsen's PostgreSQL 12.3 analysis found
  single-node histories consistent with these, which add real-time order. A
  pooler that acks early or serves stale state breaks real-time order first.
  Not for shards or replicas, where real-time order across nodes isn't
  promised."
  {:serializable    :strict-serializable
   :repeatable-read :strong-snapshot-isolation
   :read-committed  :read-committed})

(defn consistency-models
  "Models Elle checks: --consistency-models if given, else the strong model
  in pooler mode and the plain one otherwise."
  [opts]
  (or (seq (:consistency-models opts))
      [((if (= :pooler (:mode opts)) strong-consistency-model consistency-model)
        (:isolation opts))]))

(defn txn-only
  "Elle reads every client op as a transaction; hide anything else."
  [c]
  (reify checker/Checker
    (check [_ test history opts]
      (checker/check c test (h/filter-f :txn history) opts))))

(defn lost-appends-checker
  "Every element appended by an :ok txn must be in the final read of its key
  (the :final? reads, after healing). Elle catches this only under models
  with real-time order (strict-serializable, strong-snapshot-isolation): an
  acked append that never shows up has no place in the version order, so at
  read-committed, serializable or snapshot-isolation it forms no cycle. This
  makes \"acked appends lost\" visible at every isolation level and in
  --control runs."
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [txns   (filter #(and (= :txn (:f %)) (= :ok (:type %))) history)
            finals (->> (filter :final? txns)
                        (mapcat :value)
                        (reduce (fn [m [_ k v]] (assoc m k (set v))) {}))
            appends (for [op    (remove :final? txns)
                          [f k v] (:value op)
                          :when (= :append f)]
                      [op k v])
            [checked unread] ((juxt filter remove) (fn [[_ k]] (contains? finals k)) appends)
            lost   (->> checked
                        (remove (fn [[_ k v]] (contains? (finals k) v)))
                        (map (fn [[op k v]] {:index (:index op), :process (:process op),
                                             :time (:time op), :k k, :v v,
                                             :backend-pid (:backend-pid op)})))]
        {:valid?          (empty? lost)
         :checked-appends (count checked)
         :unread-appends  (count unread)
         :lost-count      (count lost)
         :lost            (vec (take 20 lost))}))))

(defn workload
  [opts]
  (let [w (append/test {:key-count          (:key-count opts 10)
                        :min-txn-length     1
                        :max-txn-length     (:max-txn-length opts 4)
                        :max-writes-per-key (:max-writes-per-key opts 32)
                        :consistency-models (consistency-models opts)})]
    (assoc w
           :client          (Client. nil nil false)
           :final-generator (FinalReads.)
           :checker (checker/compose {:elle      (txn-only (:checker w))
                                      :isolation (pc/isolation-checker)
                                      :lost      (lost-appends-checker)
                                      :backends  (pc/backend-checker)}))))
