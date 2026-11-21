(ns pgdog.jepsen.workload.bank
  "Bank transfers between accounts on different shards (M3: C4, C5).

  Accounts are rows of the `bank` table, spread round-robin over the shards
  (db/bank-accounts). A transfer is one txn:

    BEGIN ISOLATION LEVEL ...
    UPDATE bank SET balance = balance - a WHERE k = from AND balance >= a
      (0 rows: insufficient funds, the txn aborts)
    UPDATE bank SET balance = balance + a WHERE k = to
    COMMIT

  Most transfers move money between shards (--cross-frac), so they commit
  with 2PC when it is on. --auto-frac F makes that fraction of cross-shard
  transfers a single autocommit UPDATE over both accounts (transfer-auto!),
  atomic only with two_phase_commit_auto. A read selects every account with no shard key,
  which PgDog broadcasts to every shard and merges.

  Checks (bank-checker), on the final direct read of every shard after 2PC
  recovery:
  - the total is conserved and no balance is negative (atomicity: a
    transfer committed on one shard only changes the total);
  - the final read through PgDog equals the direct read.
  Reads during the run may see a wrong total: PgDog promises no global
  snapshot, so a read can see one shard before a transfer's commit and the
  other after it. Those are counted, not failed."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info]]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]]
            [jepsen.random :as rand]
            [next.jdbc :as j]
            [pgdog.jepsen.client :as pc]
            [pgdog.jepsen.db :as db]
            [pgdog.jepsen.shard :as shard]
            [pgdog.jepsen.two-pc :as two-pc]))

(defn update-count [result]
  (:next.jdbc/update-count (first result)))

(defn transfer!
  [conn {:keys [from to amount]}]
  (when (zero? (update-count
                 (j/execute! conn [(str "UPDATE " db/bank-table " SET balance = balance - ? "
                                        "WHERE k = ? AND balance >= ?")
                                   amount from amount])))
    (throw (ex-info "insufficient funds" {:type :insufficient-funds, :from from})))
  (when (zero? (update-count
                 (j/execute! conn [(str "UPDATE " db/bank-table " SET balance = balance + ? WHERE k = ?")
                                   amount to])))
    (throw (ex-info "no such account" {:type :no-account, :to to})))
  {:from from, :to to, :amount amount})

(defn transfer-auto!
  "A transfer as one autocommit statement touching both accounts' shards.
  It does not check the balance first: when the debit would go negative,
  the CHECK (balance >= 0) fails on the from-account's shard only. Atomic
  only with two_phase_commit_auto (D1: off by default in code, on in the
  docs). Postgres semantics: an error means nothing happened; through PgDog
  only class 23 / 40 errors are definite (:fail), others may come from phase
  2 (:info)."
  [test node conn-atom op]
  (let [{:keys [from to amount]} (:value op)]
    (try
      (let [^java.sql.Connection c (or @conn-atom (reset! conn-atom (pc/open test node (:shard op) {})))]
        (.setAutoCommit c true)
        (j/execute! c [(str "UPDATE " db/bank-table " SET balance = balance + "
                            "CASE WHEN k = ? THEN -? ELSE ? END WHERE k IN (?, ?)")
                       from amount amount from to])
        (assoc op :type :ok))
      (catch Throwable e
        ;; With auto 2PC the error may come from the statement (definite),
        ;; from PREPARE (class 40 / 23: definite) or from COMMIT PREPARED
        ;; (committed anyway, H4). PgDog closes the connection after a 2PC
        ;; error, so always reconnect.
        (let [state     (pc/sql-state e)
              definite? (and (instance? java.sql.SQLException e) (not (pc/connection-error? e))
                             state (or (str/starts-with? state "23") (str/starts-with? state "40")))]
          (pc/close! @conn-atom) (reset! conn-atom nil)
          (assoc op :type (if definite? :fail :info)
                 :error [(pc/error-type e) (.getMessage e)]))))))

(defn read-all!
  "Every account, by a statement with no shard key (broadcast)."
  [test conn]
  (->> (j/execute! conn [(str "SELECT k, balance FROM " db/bank-table)] (pc/jdbc-opts test))
       (map (juxt :k :balance))
       (into (sorted-map))))

(defrecord Client [node conn]
  client/Client
  (open! [_ test node] (Client. node (atom nil)))
  (setup! [_ test])

  (invoke! [_ test op]
    (case (:f op)
      :transfer (pc/run-txn! test node conn op #(transfer! % (:value op)))
      :transfer-auto (transfer-auto! test node conn op)
      :read     (let [r (pc/run-txn! test node conn op #(read-all! test %))]
                  ;; Final reads check recovery, not the old socket: retry once.
                  (if (and (:final? op) (not= :ok (:type r)) (pc/connection-loss? r))
                    (assoc (pc/run-txn! test node conn op #(read-all! test %)) :retried-error (:error r))
                    r))))

  (teardown! [_ test])
  (close! [_ test] (pc/close! @conn)))

(defn generator
  "Transfers (mostly between shards, --cross-frac) and broadcast reads, 4:1.
  Every op carries :shard (where run-txn!'s probes go: the from-account's
  shard, or a random one for reads) and :shards. --inject-errors F makes
  that fraction of cross-shard transfers fail on one shard and COMMIT anyway
  (C12)."
  [test]
  (let [accts  (db/bank-accounts test)
        by     (group-by #(shard/shard-of test %) accts)
        n      (shard/shard-count test)]
    (fn [_ _]
      (if (< (rand/double) 0.2)
        {:type :invoke, :f :read, :value nil, :shard (rand/long n), :shards (set (range n))}
        (let [from   (rand/nth accts)
              fs     (shard/shard-of test from)
              cross? (and (< 1 n) (< (rand/double) (:cross-frac test 0.8)))
              to     (rand/nth (if cross?
                                 (vec (mapcat val (dissoc by fs)))
                                 (vec (remove #{from} (by fs)))))
              shards (into (sorted-set) [fs (shard/shard-of test to)])
              auto?  (and cross? (< (rand/double) (:auto-frac test 0)))
              op     {:type :invoke, :f (if auto? :transfer-auto :transfer), :shard fs, :shards shards
                      :value {:from from, :to to, :amount (inc (rand/long (:max-transfer test 5)))}}]
          (if (and (not auto?) (< 1 (count shards)) (< (rand/double) (:inject-errors test 0)))
            (assoc op :inject-error (rand/nth (vec shards)))
            op))))))

(defn- last-value [history f]
  (->> (pc/nemesis-pairs history) (map second) (filter #(= f (:f %))) last :value))

(defn bank-checker
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [expected (* (db/bank-initial test) (count (db/bank-accounts test)))
            dump     (last-value history :shard-dump)
            direct   (when (and (map? dump) (not-any? :error (vals dump)))
                       (into (sorted-map) (for [[_ {:keys [rows]}] dump, [k v] rows] [k v])))
            reads    (filter #(and (= :read (:f %)) (= :ok (:type %)) (not (:final? %))) history)
            total    #(reduce + (vals %))
            off      (remove #(= expected (total (:value %))) reads)
            final    (->> history (filter #(and (= :read (:f %)) (:final? %) (= :ok (:type %)))) last :value)
            transfers (filter #(#{:transfer :transfer-auto} (:f %)) history)]
        (if-not direct
          {:valid? :unknown, :dump dump}
          {:valid?            (and (= expected (total direct))
                                   (not-any? neg? (vals direct))
                                   (= (set (db/bank-accounts test)) (set (keys direct)))
                                   (= direct final))
           :expected-total    expected
           :final-total       (total direct)
           :final-balances    direct
           :final-read        final
           :final-read-matches? (= direct final)
           :negative          (into {} (filter (comp neg? val)) direct)
           :transfers         (frequencies (map (juxt :f :type #(boolean (< 1 (count (:shards %)))))
                                                (remove #(= :invoke (:type %)) transfers)))
           :reads             (count reads)
           ;; Expected without a global snapshot; informational.
           :reads-wrong-total (count off)
           :read-totals       (frequencies (map total (map :value off)))})))))

(defn workload
  [opts]
  {:client          (Client. nil nil)
   :generator       (generator opts)
   :final-generator {:f :read, :final? true, :shard 0}
   :checker         (checker/compose
                      {:bank      (bank-checker)
                       :isolation (pc/isolation-checker)
                       :orphans   (two-pc/orphan-checker)})})
