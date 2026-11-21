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
                                 secondary key `sk` (a table scan), or either

  In sharded mode (M2, claim C3) every txn touches keys on one shard only:
  one Elle generator runs per shard, its keys mapped into that shard's key
  space (pgdog.jepsen.shard), and each op carries :shard. Elle checks each
  shard's sub-history on its own. Reads by sk have no shard key, so PgDog
  broadcasts them: a cross-shard read inside an otherwise single-shard txn."
  (:require [clojure.string :as str]
            [jepsen [checker :as checker]
                    [client :as client]
                    [history :as h]]
            [jepsen.generator :as gen]
            [jepsen.random :as rand]
            [jepsen.tests.cycle.append :as append]
            [elle.list-append :as la]
            [elle.consistency-model :as cm]
            [next.jdbc :as j]
            [pgdog.jepsen.client :as pc]
            [pgdog.jepsen.db :as db]
            [pgdog.jepsen.shard :as shard]
            [pgdog.jepsen.two-pc :as two-pc])
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
  "Runs one micro-op. Final reads (final? true) always read by primary key:
  they are the ground truth for the checkers, and a read by sk is broadcast
  to every shard."
  [test final? conn [f k v :as mop]]
  (let [t (table test k)]
    (case f
      :r
      (let [col (if final? "pk" (name (pick (:read-by test) [:pk :sk])))
            col (if (= "pk" col) "k" "sk")
            row (j/execute-one! conn [(str "SELECT v FROM " t " WHERE " col " = ?") k]
                                (pc/jdbc-opts test))]
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

;; ---------------------------------------------------------------------------
;; Sharded mode: one generator per shard

(defn shard-gen
  "Elle's append generator for shard s, with its local keys 0, 1, ...
  mapped to the shard's keys."
  [test elle-opts s]
  (map (fn [op]
         (-> op
             (assoc :shard s)
             (update :value (partial mapv (fn [[f k v]] [f (shard/key-of test s k) v])))))
       (la/gen elle-opts)))

(defn shard-key-tracker
  "Like cycle/max-key-tracker: keeps {shard #{key}} of every key a txn used
  in (:shard-keys ctx), for the final reads. Each key is filed under the
  shard it maps to, so cross-shard txns are covered too."
  [test]
  (fn [gen]
    (gen/track :shard-keys {}
               (fn [m {:keys [f value] :as op}]
                 (if (and (= :txn f) (or (:shard op) (:shards op)))
                   (reduce (fn [m [_ k]]
                             (update m (shard/shard-of test k) (fnil conj (sorted-set)) k))
                           m value)
                   m))
               gen)))

;; ---------------------------------------------------------------------------
;; Cross-shard mode (M3): one Elle key space spread over every shard

(defn cross-gen
  "--cross-shard: Elle's append generator over one key space, with key j
  placed on shard (mod j n) (shard/global-key), so a txn of several micro-ops
  usually spans several shards. Each op carries :shards (the shards its keys
  map to) and :shard, the first of them, where run-txn!'s probes go. With
  --inject-errors F, that fraction of multi-shard txns get :inject-error s:
  after their micro-ops a statement fails on shard s, and the client sends
  COMMIT anyway (C12)."
  [test elle-opts]
  (map (fn [op]
         (let [v      (mapv (fn [[f k v]] [f (shard/global-key test k) v]) (:value op))
               shards (into (sorted-set) (map #(shard/shard-of test (second %))) v)
               op     (assoc op :value v, :shards shards, :shard (first shards))]
           (if (and (< 1 (count shards))
                    (< (rand/double) (:inject-errors test 0)))
             (assoc op :inject-error (rand/nth (vec shards)))
             op)))
       (la/gen elle-opts)))

(defrecord ShardFinalReads []
  ;; Reads every key each shard's generator used, 8 per txn, one shard per
  ;; txn, marked :final?.
  gen/Generator
  (op [_ test ctx]
    (when-let [sk (:shard-keys ctx)]
      (gen/op (for [[s ks] (sort sk)
                    group  (partition-all 8 ks)]
                {:f :txn, :final? true, :shard s, :shards #{s},
                 :value (mapv (fn [k] [:r k nil]) group)})
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
    (let [^Connection conn (or @conn-atom (reset! conn-atom (pc/open test node (:shard op) {})))]
      (.setAutoCommit conn true)
      (.setTransactionIsolation conn Connection/TRANSACTION_READ_COMMITTED)
      (j/execute! conn ["SELECT 1"])
      (assoc op :type :fail, :error [:neighbor]))
    (catch Throwable e
      (pc/close! @conn-atom)
      (reset! conn-atom nil)
      (assoc op :type :fail, :error [:neighbor (pc/error-type e)]))))

(defn conn-for
  "The connection atom for an op. Every op shares one, except in sharded
  --control mode, where a txn connects straight to its shard's Postgres and
  each shard gets its own."
  [test conns op]
  (let [k (when (and (:control test) (= :sharded (:mode test))) (:shard op))]
    (or (get @conns k)
        (get (swap! conns #(if (contains? % k) % (assoc % k (atom nil)))) k))))

(defrecord Client [node conns neighbor?]
  client/Client
  (open! [_ test node]
    (Client. node (atom {})
             (and (:neighbors test) (odd? (swap! neighbor-counter inc)))))

  (setup! [_ test])

  (invoke! [_ test op]
    (let [conn (conn-for test conns op)
          txn! #(pc/run-txn! test node conn op
                             (fn [c] (mapv (partial mop! test (:final? op) c) (:value op))))]
      (cond
        (and neighbor? (not (:final? op)))
        (neighbor! test node conn op)

        ;; Final reads check that the system recovered, not that the
        ;; client's old socket survived: a connection killed while idle
        ;; (e.g. by a terminate-all) fails on first use. run-txn! has
        ;; dropped it by then, so retry once on a fresh connection.
        (:final? op)
        (let [r (txn!)]
          (if (and (not= :ok (:type r)) (pc/connection-loss? r))
            (assoc (txn!) :retried-error (:error r))
            r))

        :else
        (txn!))))

  (teardown! [_ test])

  (close! [_ test]
    (doseq [a (vals @conns)] (pc/close! @a))))

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
  "Models Elle checks: --consistency-models if given, else the strong model.
  In sharded mode Elle checks each shard's sub-history on its own (see
  per-shard-checker), and one Postgres per sub-history is the setting the
  strong models were found to hold in. Real-time order across shards is not
  promised and is not checked. --cross-shard histories as a whole promise
  only read-committed (see workload)."
  [opts]
  (or (seq (:consistency-models opts))
      (if (:cross-shard opts)
        [:read-committed]
        [(strong-consistency-model (:isolation opts))])))

(defn txn-only
  "Elle reads every client op as a transaction; hide anything else."
  [c]
  (reify checker/Checker
    (check [_ test history opts]
      (checker/check c test (h/filter-f :txn history) opts))))

(defn per-shard-checker
  "Runs Elle on each shard's txns separately (ops tagged :shard), writing
  each shard's Elle output under shard-<s>/."
  [elle-opts]
  (reify checker/Checker
    (check [_ test history opts]
      (let [results (into (sorted-map)
                          (for [s (range (shard/shard-count test))]
                            [s (checker/check
                                 (append/checker elle-opts) test
                                 (h/filter #(and (= :txn (:f %)) (= s (:shard %))) history)
                                 (assoc opts :subdirectory (str "shard-" s)))]))]
        {:valid? (checker/merge-valid (map :valid? (vals results)))
         :anomaly-types (->> (vals results) (mapcat :anomaly-types) distinct sort vec)
         :shards  results}))))

(defn project
  "The part of txn op on shard s: only the micro-ops whose keys map to s."
  [test s op]
  (update op :value (fn [v] (filterv #(= s (shard/shard-of test (second %))) v))))

(defn projection-checker
  "--cross-shard: each shard runs its part of a cross-shard txn as an
  ordinary local Postgres txn, so each shard's projection of the history
  (every txn cut down to the micro-ops on that shard) must satisfy the
  per-shard model, even though the whole history only promises
  read-committed (no global snapshot). A :fail txn's part must be aborted on
  every shard: a partial commit shows up here as an aborted read (G1a).
  Output under <subdir>-<s>/."
  [elle-opts subdir]
  (reify checker/Checker
    (check [_ test history opts]
      (let [results (into (sorted-map)
                          (for [s (range (shard/shard-count test))]
                            [s (checker/check
                                 (append/checker elle-opts) test
                                 (->> history
                                      ;; Final reads from runs before they
                                      ;; carried :shards have only :shard.
                                      (h/filter #(and (= :txn (:f %))
                                                      (or (contains? (:shards %) s)
                                                          (and (:final? %) (= s (:shard %))))))
                                      (h/map (partial project test s)))
                                 (assoc opts :subdirectory (str subdir "-" s)))]))]
        {:valid?        (checker/merge-valid (map :valid? (vals results)))
         :anomaly-types (->> (vals results) (mapcat :anomaly-types) distinct sort vec)
         :shards        results}))))

(defn drop-allowed-timeouts
  "Elle searches for every cycle type whatever the models, and a search that
  times out makes the result :unknown. Cross-shard histories have big SCCs
  full of G-single / G2 cycles, which read-committed allows, so the search
  for one of those often times out on clean runs. This drops timeouts for
  anomaly types the models allow (keeping them as :informational-timeouts)
  and recomputes :valid?. G0 / G1c are searched first and PL-1 / PL-2
  cycle-exists cases are computed outside the timeout, so a prohibited cycle
  is still reported."
  [c models]
  (reify checker/Checker
    (check [_ test history opts]
      (let [r          (checker/check c test history opts)
            prohibited (set (cm/anomalies-prohibited-by models))
            tos        (get-in r [:anomalies :cycle-search-timeout])
            {keep true drop false} (group-by #(boolean (prohibited (:anomaly-spec-type %))) tos)]
        (if (empty? drop)
          r
          (let [anoms (if (seq keep)
                        (assoc (:anomalies r) :cycle-search-timeout (vec keep))
                        (dissoc (:anomalies r) :cycle-search-timeout))
                types (sort (keys anoms))]
            (assoc r
                   :anomalies              anoms
                   :anomaly-types          types
                   :informational-timeouts (vec drop)
                   :valid?                 (cond (some #(and (prohibited %) (not= :cycle-search-timeout %)) types) false
                                                 (seq keep) :unknown
                                                 :else      true))))))))

(defn informational
  "Runs checker c but never fails the test: its verdict is kept as
  :checked-valid?. For properties the docs don't promise."
  [c]
  (reify checker/Checker
    (check [_ test history opts]
      (let [r (checker/check c test history opts)]
        (assoc r :valid? true, :checked-valid? (:valid? r))))))

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
  (let [elle-opts {:key-count          (:key-count opts 10)
                   ;; Cross-shard: at least two micro-ops, so most txns
                   ;; span shards.
                   :min-txn-length     (if (:cross-shard opts) 2 1)
                   :max-txn-length     (:max-txn-length opts 4)
                   :max-writes-per-key (:max-writes-per-key opts 32)
                   :consistency-models (consistency-models opts)}
        common    {:isolation (pc/isolation-checker)
                   :lost      (lost-appends-checker)
                   :backends  (pc/backend-checker)}]
    (cond
      (:cross-shard opts)
      ;; No global snapshot or commit order across shards: the whole
      ;; history is only read-committed (G0, G1a/b/c). Each shard's
      ;; projection must still meet the per-shard model; the strong
      ;; (real-time) per-shard model is informational: PgDog may ack before
      ;; phase 2 reaches every shard ("eventual consistency").
      (let [shard-opts (assoc elle-opts :consistency-models
                              (or (seq (:shard-consistency-models opts))
                                  [(consistency-model (:isolation opts))]))
            rt-opts    (assoc elle-opts :consistency-models
                              [(strong-consistency-model (:isolation opts))])]
        {:client          (Client. nil nil false)
         :generator       (cross-gen opts elle-opts)
         :wrap-generator  (shard-key-tracker opts)
         :final-generator (ShardFinalReads.)
         :checker         (checker/compose
                            (assoc common
                                   :elle          (txn-only (drop-allowed-timeouts
                                                              (append/checker elle-opts)
                                                              (:consistency-models elle-opts)))
                                   :elle-shards   (projection-checker shard-opts "shard")
                                   :realtime      (informational (projection-checker rt-opts "realtime"))
                                   :placement     (pc/placement-checker)
                                   :atomicity     (two-pc/atomicity-checker)
                                   :orphans       (two-pc/orphan-checker)))})

      (= :sharded (:mode opts))
      {:client          (Client. nil nil false)
       :generator       (gen/mix (mapv (partial shard-gen opts elle-opts)
                                       (range (shard/shard-count opts))))
       :wrap-generator  (shard-key-tracker opts)
       :final-generator (ShardFinalReads.)
       :checker         (checker/compose
                          (assoc common
                                 :elle      (per-shard-checker elle-opts)
                                 :placement (pc/placement-checker)))}
      :else
      (let [w (append/test elle-opts)]
        (assoc w
               :client          (Client. nil nil false)
               :final-generator (FinalReads.)
               :checker (checker/compose (assoc common :elle (txn-only (:checker w)))))))))
