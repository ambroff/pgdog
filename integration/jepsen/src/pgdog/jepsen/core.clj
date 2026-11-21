(ns pgdog.jepsen.core
  "Entry point for the PgDog Jepsen suite. See PLAN.org at the repo root."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info]]
            [jepsen [checker :as checker]
                    [cli :as cli]
                    [generator :as gen]
                    [role :as role]
                    [tests :as tests]]
            [elle.consistency-model :as cm]
            [jepsen.checker.timeline :as timeline]
            [jepsen.nemesis :as n]
            [jepsen.nemesis.combined :as nc]
            [jepsen.os.debian :as debian]
            [jepsen.store :as store]
            [clojure.pprint]
            [pgdog.jepsen.client :as pc]
            [pgdog.jepsen.db :as db]
            [pgdog.jepsen.nemesis :as pn]
            [pgdog.jepsen.recheck :as recheck]
            [pgdog.jepsen.two-pc :as two-pc]
            [pgdog.jepsen.workload.append :as append]
            [pgdog.jepsen.workload.bank :as bank]
            [pgdog.jepsen.workload.session-leak :as session-leak]))

(def workloads
  {:append       append/workload
   :bank         bank/workload
   :session-leak session-leak/workload})

(def nemesis-faults
  "Faults accepted by --nemesis. kill/pause are per role; partition acts on
  any nodes (e.g. cutting PgDog off from a shard); partition-clients cuts the
  Jepsen clients off from whatever they connect to; admin runs RELOAD /
  RECONNECT and pause-pool runs PAUSE / RESUME on PgDog's admin database."
  #{:kill-pgdog :pause-pgdog :kill-postgres :pause-postgres :partition
    :partition-clients :admin :pause-pool
    ;; Backend faults on the Postgres side (see pn/backend-package):
    :terminate-backend :kill-backend :restart-postgres})

(defn roles
  "Splits the node list: the first --pgdog-count nodes run PgDog, the rest run
  Postgres. Pooler mode uses only one Postgres node."
  [{:keys [nodes pgdog-count mode]}]
  (let [[pgdog pg] (split-at pgdog-count nodes)
        pg         (if (= :pooler mode) (take 1 pg) pg)]
    (assert (seq pgdog) "need at least one PgDog node")
    (assert (seq pg) "need at least one Postgres node")
    {:pgdog (vec pgdog), :postgres (vec pg)}))

(defn nemesis-package
  [opts db]
  (let [faults   (set (:nemesis opts))
        base     {:db db, :interval (:nemesis-interval opts)}
        per-role (fn [role kill pause]
                   (let [fs (cond-> #{}
                              (faults kill)  (conj :kill)
                              (faults pause) (conj :pause))]
                     (when (seq fs)
                       (role/restrict-nemesis-package
                         role
                         (nc/nemesis-package
                           (assoc base
                                  :faults fs
                                  :kill  {:targets [:one :all]}
                                  :pause {:targets [:one :all]}))))))
        pkgs     (->> [(per-role :pgdog :kill-pgdog :pause-pgdog)
                       (per-role :postgres :kill-postgres :pause-postgres)
                       (when (faults :partition)
                         (nc/nemesis-package
                           (assoc base
                                  :faults #{:partition}
                                  :partition {:targets [:one :majority]})))
                       (when (faults :partition-clients)
                         (pn/client-partition-package base))
                       (when (faults :admin)
                         (pn/admin-package base))
                       (when (faults :pause-pool)
                         (pn/pool-pause-package base))
                       (when (some faults (keys pn/backend-faults))
                         (pn/backend-package (assoc base :faults faults)))]
                      (remove nil?))]
    (if (seq pkgs)
      (nc/compose-packages pkgs)
      (nc/nemesis-package (assoc base :faults #{})))))

(defn default-isolation-mode
  "How txns set their isolation level unless --isolation-mode says
  otherwise: SET TRANSACTION in pooler mode (M1), BEGIN ISOLATION LEVEL in
  sharded mode, the only way a txn stays on one shard (see
  client/run-txn!)."
  [opts]
  (if (= :sharded (:mode opts)) :begin :txn))

(defn test-name
  "Store directory name, so no spaces. Options at their defaults are left out."
  [opts]
  (str/join "-"
            (cond-> ["pgdog" (name (:mode opts)) (name (:workload opts))]
              (#{:append :bank} (:workload opts))
              (conj (name (:isolation opts)))

              (and (= :session-leak (:workload opts))
                   (not= session-leak/mechanisms (:session-mechanisms opts)))
              (conj (str/join "+" (map name (:session-mechanisms opts))))

              (and (= :sharded (:mode opts)) (not= :list (:sharding opts)))
              (conj (name (:sharding opts)))

              (:cross-shard opts)
              (conj "xshard")

              (:two-pc opts)
              (conj "2pc")

              (some? (:two-pc-auto opts))
              (conj (str "auto-" (:two-pc-auto opts)))

              (false? (:node-id opts))
              (conj "no-node-id")

              (not= :keep (:wal-mode opts))
              (conj (str "wal-" (name (:wal-mode opts))))

              (some? (:wal-fsync-interval opts))
              (conj (str "fsync" (:wal-fsync-interval opts)))

              (pos? (:inject-errors opts 0))
              (conj (str "inject" (:inject-errors opts)
                         (when (not= :server (:inject-error-kind opts)) (str "-" (name (:inject-error-kind opts))))))

              (pos? (:auto-frac opts 0))
              (conj (str "autofrac" (:auto-frac opts)))

              (pos? (:lock-timeout opts 0))
              (conj (str "lt" (:lock-timeout opts)))

              (and (= :append (:workload opts))
                   (not= (default-isolation-mode opts) (:isolation-mode opts)))
              (conj (case (:isolation-mode opts)
                      :session "session-iso"
                      :txn     "set-txn"
                      :begin   "begin-iso"))

              (not= 1 (:tables opts))
              (conj (str "tables" (:tables opts)))

              (not= :upsert (:append-path opts))
              (conj (name (:append-path opts)))

              (not= :unqualified (:jdbc-builder opts))
              (conj (str "jdbc-" (name (:jdbc-builder opts))))

              (not= :pk (:read-by opts))
              (conj (str "read-" (name (:read-by opts))))

              (not= 10 (:key-count opts))
              (conj (str "keys" (:key-count opts)))

              (not= 32 (:max-writes-per-key opts))
              (conj (str "mwpk" (:max-writes-per-key opts)))

              (not= 4 (:pool-size opts))
              (conj (str "pool" (:pool-size opts)))

              (:strict-commit-errors opts)
              (conj "strict")

              true
              (conj (str "c" (:concurrency opts)))

              (not= 60 (:time-limit opts))
              (conj (str "t" (:time-limit opts)))

              (seq (:consistency-models opts))
              (conj (str/join "+" (map name (:consistency-models opts))))

              (:neighbors opts)
              (conj "neighbors")

              (not= :extended (:query-mode opts))
              (conj (name (:query-mode opts)))

              (not= 5 (:prepare-threshold opts))
              (conj (str "prep" (:prepare-threshold opts)))

              (not= :auto (:query-parser opts))
              (conj (str "parser-" (name (:query-parser opts))))

              (not= :transaction (:pooler-mode opts))
              (conj (name (:pooler-mode opts)))

              (:control opts)
              (conj "control")

              (seq (:nemesis opts))
              (conj (str/join "+" (map name (:nemesis opts)))))))

(defn pgdog-test
  [opts]
  (let [roles    (roles opts)
        nodes    (vec (concat (:pgdog roles) (:postgres roles)))
        opts     (assoc opts :roles roles :nodes nodes)
        db       (db/db)
        workload ((workloads (:workload opts)) opts)
        pkg      (nemesis-package opts db)
        wrap     (or (:wrap-generator workload) identity)
        sharded? (and (= :sharded (:mode opts)) (#{:append :bank} (:workload opts)))]
    (merge tests/noop-test
           opts
           {:name      (test-name opts)
            :os        debian/os
            :db        db
            :client    (role/restrict-client :pgdog (:client workload))
            :nemesis   (n/compose [(:nemesis pkg) (pn/pool-stats-nemesis)
                                   (pn/shard-dump-nemesis) (two-pc/nemesis)])
            :checker   (checker/compose
                         {:perf       (checker/perf {:nemeses (:perf pkg)})
                          :stats      (checker/stats)
                          :exceptions (checker/unhandled-exceptions)
                          :pools      (pc/pool-checker)
                          :timeline   (timeline/html)
                          :workload   (:checker workload)})
            :generator (wrap
                         (gen/phases
                           (if sharded?
                             (gen/nemesis {:type :info, :f :xact-stats, :value nil})
                             [])
                           (->> (:generator workload)
                                (gen/stagger (/ (:rate opts)))
                                (gen/nemesis (:generator pkg))
                                (gen/time-limit (:time-limit opts)))
                           (gen/log "Healing cluster")
                           (gen/nemesis (:final-generator pkg))
                           (gen/log "Waiting for recovery")
                           (gen/sleep (:recovery-time opts))
                           ;; PgDog finishes in-doubt 2PC txns in the
                           ;; background; give it --two-pc-deadline s.
                           (if sharded?
                             (gen/phases (gen/log "Waiting for 2PC recovery")
                                         (gen/nemesis {:type :info, :f :await-2pc, :value nil}))
                             [])
                           (gen/clients (:final-generator workload))
                           (if sharded?
                             (gen/phases (gen/log "Reading every shard directly")
                                         (gen/nemesis {:type :info, :f :shard-dump, :value nil})
                                         (gen/nemesis {:type :info, :f :prepared-xacts, :value nil}))
                             [])
                           (gen/log "Checking that pools drained")
                           (gen/nemesis {:type :info, :f :pool-stats, :value nil})))})))

(defn parse-faults [s]
  (->> (str/split s #",")
       (remove str/blank?)
       (mapv keyword)))

(def opt-spec
  [["-w" "--workload NAME" "Workload to run"
    :default :append
    :parse-fn keyword
    :validate [workloads (cli/one-of workloads)]]

   [nil "--mode MODE" "pooler (one Postgres) or sharded (every Postgres node is a shard)"
    :default :pooler
    :parse-fn keyword
    :validate [#{:pooler :sharded} "must be pooler or sharded"]]

   [nil "--pgdog-count N" "How many of the nodes run PgDog; the rest run Postgres"
    :default 2
    :parse-fn parse-long]

   [nil "--control" "Bypass PgDog: clients connect straight to Postgres (in sharded mode, to the txn's shard)"
    :default false]

   [nil "--sharding KIND" (str "sharded mode: how PgDog places keys. list ([[sharded_tables.mapping]] values), "
                               "range (mapping start/end), or hash (PgDog's default hasher)")
    :default :list
    :parse-fn keyword
    :validate [#{:list :range :hash} "must be list, range or hash"]]

   [nil "--jdbc-builder KIND" (str "next.jdbc row builder: unqualified (column labels only) or qualified "
                                   "(next.jdbc's default; pgjdbc runs pg_catalog lookups inside the txn, which PgDog "
                                   "routes to the client's sticky shard)")
    :default :unqualified
    :parse-fn keyword
    :validate [#{:unqualified :qualified} "must be unqualified or qualified"]]

   [nil "--list-size N" "--sharding list: keys listed per shard; a run that needs more fails"
    :default 20000
    :parse-fn parse-long]

   [nil "--isolation LEVEL" "serializable, repeatable-read or read-committed"
    :default :serializable
    :parse-fn keyword
    :validate [#{:serializable :repeatable-read :read-committed} "unknown isolation level"]]

   [nil "--tables N" "append: spread keys over N tables (by hash of the key)"
    :default 1
    :parse-fn parse-long
    :validate [pos? "must be positive"]]

   [nil "--append-path PATH" "append: upsert (INSERT ON CONFLICT), update-insert (UPDATE, then INSERT in a SAVEPOINT), or mixed"
    :default :upsert
    :parse-fn keyword
    :validate [#{:upsert :update-insert :mixed} "must be upsert, update-insert or mixed"]]

   [nil "--read-by KEY" "append: read by pk, by the unindexed secondary key sk (table scan), or mixed"
    :default :pk
    :parse-fn keyword
    :validate [#{:pk :sk :mixed} "must be pk, sk or mixed"]]

   [nil "--key-count N" "append: keys active at once"
    :default 10
    :parse-fn parse-long]

   [nil "--max-writes-per-key N" "append: appends per key before it is retired for a fresh one (lower = more fresh inserts)"
    :default 32
    :parse-fn parse-long]

   [nil "--max-txn-length N" "append: micro-ops per transaction"
    :default 4
    :parse-fn parse-long]

   [nil "--consistency-models LIST" (str "append: Elle models to check, comma-separated. Default: strict-serializable / "
                                         "strong-snapshot-isolation / read-committed in pooler mode, plain serializable / "
                                         "snapshot-isolation / read-committed otherwise")
    :parse-fn parse-faults
    :validate [#(try (cm/anomalies-prohibited-by %) true (catch Exception _ false))
               "unknown Elle consistency model"]]

   [nil "--isolation-mode MODE" (str "txn: SET TRANSACTION per txn; session: JDBC setTransactionIsolation; "
                                     "begin: BEGIN ISOLATION LEVEL with autocommit on. "
                                     "Default: txn in pooler mode, begin in sharded mode")
    :parse-fn keyword
    :validate [#{:txn :session :begin} "must be txn, session or begin"]]

   [nil "--query-mode MODE" "pgjdbc preferQueryMode: extended or simple"
    :default :extended
    :parse-fn keyword
    :validate [#{:extended :simple :extendedForPrepared} "unknown query mode"]]

   [nil "--prepare-threshold N" "pgjdbc prepareThreshold; 0 disables named prepared statements"
    :default 5
    :parse-fn parse-long]

   [nil "--neighbors" (str "append: every other client is a neighbor that only sets its session to READ COMMITTED "
                           "through JDBC on the same pool (pairs with --isolation-mode session)")
    :default false]

   [nil "--strict-commit-errors" "Treat every non-connection error on COMMIT as a definite failure (H4)"
    :default false]

   [nil "--two-pc" "Enable two_phase_commit"
    :default false]

   [nil "--cross-shard" (str "sharded append: one key space spread over every shard (key j on shard j mod n), "
                             "so most txns span shards (M3). Checks Elle at read-committed on the whole history, "
                             "each shard's projection at the per-shard model, final-state atomicity and orphaned "
                             "prepared txns")
    :default false]

   [nil "--inject-errors FRAC" (str "--cross-shard: fraction of multi-shard txns that hit an error on one of their "
                                    "shards (division by zero) and then send COMMIT anyway (C12)")
    :default 0
    :parse-fn parse-double]

   [nil "--accounts N" "bank: number of accounts (spread round-robin over the shards)"
    :default 8
    :parse-fn parse-long]

   [nil "--total-amount N" "bank: total money, split evenly over the accounts"
    :default 800
    :parse-fn parse-long]

   [nil "--max-transfer N" "bank: largest transfer"
    :default 5
    :parse-fn parse-long]

   [nil "--auto-frac FRAC" "bank: fraction of cross-shard transfers done as one autocommit UPDATE (two_phase_commit_auto)"
    :default 0
    :parse-fn parse-double]

   [nil "--cross-frac FRAC" "bank: fraction of transfers between accounts on different shards"
    :default 0.8
    :parse-fn parse-double]

   [nil "--inject-error-kind KIND" (str "--inject-errors: server (Postgres rejects a statement on one shard), "
                                        "syntax (a syntax error, which PgDog's parser answers itself), or mixed")
    :default :server
    :parse-fn keyword
    :validate [#{:server :syntax :mixed} "must be server, syntax or mixed"]]

   [nil "--wal-mode MODE" (str "PgDog 2PC WAL across PgDog restarts: keep, wipe (empty WAL dir on every restart, H2), "
                               "or lazyfs (WAL dir on LazyFS; a kill also loses unfsynced writes, H3)")
    :default :keep
    :parse-fn keyword
    :validate [#{:keep :wipe :lazyfs} "must be keep, wipe or lazyfs"]]

   [nil "--wal-fsync-interval MS" "two_phase_commit_wal_fsync_interval (PgDog default 0: fsync on every write)"
    :parse-fn parse-long]

   [nil "--drain-timeout SECONDS" "How long the final pool check waits for every PgDog pool to drain"
    :default 15
    :parse-fn parse-long]
   [nil "--two-pc-deadline SECONDS" "After healing, how long to wait for PgDog to resolve prepared txns"
    :default 60
    :parse-fn parse-long]

   [nil "--lock-timeout MS" (str "Postgres lock_timeout. Cross-shard txns can deadlock across shards, which no "
                                 "Postgres detects; 0 waits forever (until the client's socket timeout)")
    :default 0
    :parse-fn parse-long]

   [nil "--shard-consistency-models LIST" "--cross-shard: Elle models for each shard's projection (default: serializable / snapshot-isolation / read-committed by isolation level)"
    :parse-fn parse-faults
    :validate [#(try (cm/anomalies-prohibited-by %) true (catch Exception _ false))
               "unknown Elle consistency model"]]

   [nil "--two-pc-auto BOOL" "Set two_phase_commit_auto explicitly"
    :parse-fn parse-boolean]

   [nil "--no-node-id" "Start PgDog without NODE_ID (H1)"
    :id :node-id
    :default true
    :assoc-fn (fn [m k _] (assoc m k false))]

   [nil "--session-mechanisms LIST" (str "session-leak: comma-separated ways to set session state: "
                                         (str/join ", " (map name session-leak/mechanisms)))
    :default session-leak/mechanisms
    :parse-fn parse-faults
    :validate [#(and (seq %) (every? (set session-leak/mechanisms) %)) "unknown mechanism"]]

   [nil "--pooler-mode MODE" "PgDog pooler_mode: transaction, session or statement"
    :default :transaction
    :parse-fn keyword
    :validate [#{:transaction :session :statement} "unknown pooler mode"]]

   [nil "--query-parser LEVEL" "PgDog query_parser: auto, on, off, session_control, session_control_and_locks"
    :default :auto
    :parse-fn keyword
    :validate [#{:auto :on :off :session_control :session_control_and_locks} "unknown level"]]

   [nil "--pool-size N" "PgDog default_pool_size; keep it below concurrency so server connections are shared"
    :default 4
    :parse-fn parse-long]

   [nil "--pg-version N" "Postgres major version (from PGDG)"
    :default "17"]

   [nil "--pgdog-bin PATH" "Local PgDog binary to upload"
    :default "../../target/release/pgdog"]

   [nil "--nemesis FAULTS" (str "Comma-separated faults: " (str/join ", " (map name (sort nemesis-faults))))
    :default []
    :parse-fn parse-faults
    :validate [(partial every? nemesis-faults) "unknown fault"]]

   [nil "--nemesis-interval SECONDS" "Mean seconds between nemesis operations"
    :default 10
    :parse-fn parse-double]

   [nil "--rate HZ" "Approximate client ops per second, across all clients"
    :default 100
    :parse-fn parse-double]

   [nil "--recovery-time SECONDS" "Pause after healing, before final reads"
    :default 10
    :parse-fn parse-long]])

(defn test-fn
  "Fills in defaults the stock Jepsen CLI doesn't know about."
  [opts]
  (when (and (:cross-shard opts) (not= :sharded (:mode opts)))
    (throw (IllegalArgumentException. "--cross-shard needs --mode sharded")))
  (when (and (:control opts) (= :sharded (:mode opts))
             (or (:cross-shard opts) (= :bank (:workload opts))))
    (throw (IllegalArgumentException.
             "--control can't run cross-shard txns: it connects to one shard per txn")))
  (-> opts
      (update :isolation-mode #(or % (default-isolation-mode opts)))
      (update-in [:ssh :private-key-path]
                 #(or % (str (System/getProperty "user.home") "/.ssh/id_jepsen")))
      pgdog-test))

(defn recheck-workload-cmd!
  "Re-runs a stored test's whole workload checker (as the current code builds
  it from the test's options) on its history, and writes the result to
  results-recheck.edn in the store dir. Elle output is rewritten in place.
  For fixing checkers without re-running tests."
  [{:keys [arguments]}]
  (doseq [dir arguments]
    (let [r (try
              (let [test (store/test (str dir))
                    wl   ((workloads (:workload test)) test)
                    r    (checker/check (:checker wl) test (:history test) {})]
                (spit (str dir "/results-recheck.edn") (with-out-str (clojure.pprint/pprint r)))
                r)
              (catch Exception e {:valid? :error, :error (.getMessage e)}))]
      (println (str/join "\t" (cons (pr-str (:valid? r))
                                     (concat (for [[k v] (sort-by key (dissoc r :valid?))
                                                   :when (map? v)]
                                               (str (name k) "=" (pr-str (:valid? v))))
                                             [dir]))))
      (flush))))

(defn -main
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn  test-fn
                                         :opt-spec opt-spec})
                   (cli/serve-cmd)
                   {"recheck" {:opt-spec [[nil "--consistency-models LIST"
                                           "Elle models, comma-separated (default: strong model for the test's isolation level)"
                                           :parse-fn parse-faults]
                                          [nil "--cycle-search-timeout MS"
                                           "Elle's per-anomaly cycle search budget (Elle default 1000)"
                                           :parse-fn parse-long]]
                               :usage    "Usage: lein run recheck [--consistency-models LIST] [--cycle-search-timeout MS] STORE_DIR..."
                               :run      recheck/run-cmd!}
                    "recheck-workload" {:opt-spec []
                                        :usage    "Usage: lein run recheck-workload STORE_DIR... (re-runs the whole workload checker)"
                                        :run      recheck-workload-cmd!}
                    "reanalyze" {:opt-spec []
                                 :usage    "Usage: lein run reanalyze STORE_DIR... (re-runs the backends/isolation/lost/placement checkers)"
                                 :run      recheck/reanalyze-cmd!}})
            args))
