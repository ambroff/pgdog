(ns pgdog.jepsen.two-pc
  "Cross-shard two-phase commit (M3: C4, C5, C12, H1-H5).

  - A nemesis that waits for PgDog's 2PC recovery after the faults heal
    (:await-2pc), and records what is still prepared on each shard
    (:prepared-xacts) and what each PgDog still tracks (SHOW TRANSACTIONS).
  - orphan-checker: after recovery, no shard may hold a prepared transaction
    with one of PgDog's gids. One left behind keeps its row locks forever and,
    if its client was acked, is a lost write.
  - atomicity-checker: in the final direct read of every shard, each txn's
    appends are present on all of its shards or on none. :ok txns must be
    present everywhere, :fail txns nowhere, :info either way."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]
                    [nemesis :as n]
                    [role :as role]]
            [next.jdbc :as j]
            [next.jdbc.result-set :as rs]
            [pgdog.jepsen.client :as pc]
            [pgdog.jepsen.db :as db]
            [pgdog.jepsen.nemesis :as pn]
            [pgdog.jepsen.shard :as shard]))

(def gid-prefix
  "Every gid PgDog prepares starts with this (two_pc/transaction.rs)."
  "__pgdog_2pc_")

(defn prepared-xacts
  "PgDog's prepared transactions on Postgres `node`, read directly:
  [{:gid :age-s :database :owner}], or {:error msg} if the node can't be
  read."
  [node]
  (try
    (with-open [^java.sql.Connection conn (pn/direct-conn node)]
      (j/execute! conn [(str "SELECT gid, database, owner, "
                             "extract(epoch FROM now() - prepared)::float8 AS age_s "
                             "FROM pg_prepared_xacts WHERE left(gid, " (count gid-prefix) ") = ? "
                             "ORDER BY prepared")
                        gid-prefix]
                  {:builder-fn rs/as-unqualified-maps}))
    (catch Exception e
      {:error (.getMessage e)})))

(defn all-prepared
  "{node prepared-xacts} over every shard."
  [test]
  (into (sorted-map)
        (map (fn [node] [node (prepared-xacts node)]))
        (role/nodes test :postgres)))

(defn show-transactions
  "{pgdog-node rows} of SHOW TRANSACTIONS on each PgDog's admin database:
  the 2PC transactions PgDog itself still tracks."
  [test]
  (into (sorted-map)
        (map (fn [node]
               [node (try (mapv (fn [row] (into {} (map (fn [[k v]] [(keyword (name k)) v])) row))
                                (pc/admin! node "SHOW TRANSACTIONS"))
                          (catch Exception e {:error (.getMessage e)}))]))
        (role/nodes test :pgdog)))

(defn pending?
  "Is anything PgDog prepared still unresolved on some shard? A shard that
  can't be read counts as pending."
  [prepared]
  (some (fn [v] (or (map? v) (seq v))) (vals prepared)))

(defn await-2pc!
  "Polls every shard until none holds a PgDog-prepared transaction, or
  `deadline-s` passes. PgDog finishes in-doubt txns in the background (and
  after a restart, from its WAL), so this is how long recovery took, or what
  it left behind."
  [test deadline-s]
  (let [t0       (System/nanoTime)
        deadline (+ t0 (* deadline-s 1e9))]
    (loop [polls 1]
      (let [p (all-prepared test)]
        (cond
          (not (pending? p))
          {:resolved? true, :waited-s (/ (- (System/nanoTime) t0) 1e9), :polls polls}

          (< (System/nanoTime) deadline)
          (do (Thread/sleep 500) (recur (inc polls)))

          :else
          {:resolved?         false
           :waited-s          (/ (- (System/nanoTime) t0) 1e9)
           :polls             polls
           :prepared          p
           :show-transactions (when-not (:control test) (show-transactions test))})))))

(defn nemesis
  "Ops:
    {:f :await-2pc}       wait (up to --two-pc-deadline s) for recovery
    {:f :prepared-xacts}  record what is still prepared, and SHOW TRANSACTIONS
  Both only observe."
  []
  (reify
    n/Reflection
    (fs [_] [:await-2pc :prepared-xacts])

    n/Nemesis
    (setup! [this test] this)

    (invoke! [_ test {:keys [f] :as op}]
      (case f
        :await-2pc
        (assoc op :value (await-2pc! test (:two-pc-deadline test 60)))

        :prepared-xacts
        (assoc op :value {:prepared          (all-prepared test)
                          :show-transactions (when-not (:control test) (show-transactions test))})))

    (teardown! [_ test])))

(defn- last-value
  [history f]
  (->> (pc/nemesis-pairs history) (map second) (filter #(= f (:f %))) last :value))

(defn orphan-checker
  "After the recovery deadline and the final reads, no shard may hold a
  prepared transaction with one of PgDog's gids. Reports what :await-2pc saw
  at its deadline and what was still there at the end."
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [await  (last-value history :await-2pc)
            final  (last-value history :prepared-xacts)
            prep   (:prepared final)
            orphans (for [[node xs] prep, :when (sequential? xs), x xs] (assoc x :node node))
            errors (into {} (keep (fn [[n v]] (when (map? v) [n (:error v)]))) prep)]
        {:valid?            (cond (nil? final)  :unknown
                                  (seq orphans) false
                                  (seq errors)  :unknown
                                  :else         true)
         :recovery          (dissoc await :prepared :show-transactions)
         :orphan-count      (count orphans)
         :orphans-by-node   (frequencies (map :node orphans))
         :orphans           (vec (take 20 orphans))
         :read-errors       errors
         :show-transactions (:show-transactions final)}))))

;; ---------------------------------------------------------------------------
;; Final-state atomicity

(defn parse-list [s]
  (when s (set (map parse-long (str/split s #",")))))

(defn dump-values
  "{k #{appended values}} from the :shard-dump op, read directly from every
  shard."
  [dump]
  (into {}
        (for [[_ {:keys [rows]}] dump
              [k v] rows]
          [k (parse-list v)])))

(defn txn-outcome
  "For one txn completion: {shard [present absent]} over its appends, and an
  overall :outcome, :committed (every append present), :absent (none) or
  :partial."
  [test values op]
  (let [per (reduce (fn [m [f k v]]
                      (if (= :append f)
                        (let [s (shard/shard-of test k)
                              here? (contains? (get values k #{}) v)]
                          (update m s (fnil (fn [[p a]] (if here? [(inc p) a] [p (inc a)])) [0 0])))
                        m))
                    {} (:value op))
        p (reduce + (map first (vals per)))
        a (reduce + (map second (vals per)))]
    {:shards  per
     :outcome (cond (zero? (+ p a)) :no-appends
                    (zero? a)       :committed
                    (zero? p)       :absent
                    :else           :partial)}))

(defn atomicity-checker
  "Each txn's appends must be all present or all absent in the final direct
  read of every shard (the :shard-dump op), whatever the txn's type:

  - :partial    some appends present, some absent: an atomicity violation
                (C4/C5). Split by whether the txn spanned shards.
  - :ok absent / partial:   an acked txn lost (some of) its writes.
  - :fail present / partial: a txn the client was told failed committed
                (H4, C12; Elle sees it as an aborted read).
  - :info: either way, but never partial.

  Orphaned prepared txns (see orphan-checker) show up here as absent or
  partial: their writes are not visible."
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [dump (last-value history :shard-dump)]
        (if-not (and (map? dump) (not-any? :error (vals dump)))
          {:valid? :unknown, :dump-errors (when (map? dump)
                                            (into {} (keep (fn [[n v]] (when (:error v) [n (:error v)]))) dump))}
          (let [values (dump-values dump)
                txns   (filter #(and (= :txn (:f %)) (integer? (:process %))
                                     (#{:ok :fail :info} (:type %))
                                     (not (:final? %))
                                     (not= :neighbor (first (:error %))))
                               history)
                ;; A :fail / :info completion carries the planned mops (run-txn!
                ;; completes the invocation op); an :ok one the executed mops.
                ;; The appends are the same either way.
                rows   (for [op txns
                             :let [o (txn-outcome test values op)]
                             :when (not= :no-appends (:outcome o))]
                         (assoc o :op op :x-shard? (< 1 (count (:shards o)))))
                bad?   (fn [{:keys [op outcome]}]
                         (or (= :partial outcome)
                             (and (= :ok (:type op)) (not= :committed outcome))
                             (and (= :fail (:type op)) (not= :absent outcome))))
                bad    (filter bad? rows)
                brief  (fn [{:keys [op outcome shards]}]
                         (merge (select-keys op [:index :process :time :type :value :error :shards])
                                {:outcome outcome, :per-shard shards}))]
            {:valid?       (empty? bad)
             :txns         (count rows)
             :x-shard-txns (count (filter :x-shard? rows))
             :outcomes     (frequencies (map (juxt (comp :type :op) :outcome :x-shard?) rows))
             :partial-count       (count (filter #(= :partial (:outcome %)) rows))
             :ok-lost-count       (count (filter #(and (= :ok (:type (:op %))) (not= :committed (:outcome %))) rows))
             :fail-visible-count  (count (filter #(and (= :fail (:type (:op %))) (not= :absent (:outcome %))) rows))
             :bad-count    (count bad)
             :bad          (vec (map brief (take 20 bad)))}))))))
