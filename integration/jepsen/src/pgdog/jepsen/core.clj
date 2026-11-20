(ns pgdog.jepsen.core
  "Entry point for the PgDog Jepsen suite. See PLAN.org at the repo root."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info]]
            [jepsen [checker :as checker]
                    [cli :as cli]
                    [generator :as gen]
                    [role :as role]
                    [tests :as tests]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.nemesis.combined :as nc]
            [jepsen.os.debian :as debian]
            [pgdog.jepsen.db :as db]
            [pgdog.jepsen.nemesis :as pn]
            [pgdog.jepsen.workload.append :as append]
            [pgdog.jepsen.workload.session-leak :as session-leak]))

(def workloads
  {:append       append/workload
   :session-leak session-leak/workload})

(def nemesis-faults
  "Faults accepted by --nemesis. kill/pause are per role; partition acts on
  any nodes (e.g. cutting PgDog off from a shard); partition-clients cuts the
  Jepsen clients off from whatever they connect to; admin runs RELOAD /
  RECONNECT and pause-pool runs PAUSE / RESUME on PgDog's admin database."
  #{:kill-pgdog :pause-pgdog :kill-postgres :pause-postgres :partition
    :partition-clients :admin :pause-pool})

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
                         (pn/pool-pause-package base))]
                      (remove nil?))]
    (if (seq pkgs)
      (nc/compose-packages pkgs)
      (nc/nemesis-package (assoc base :faults #{})))))

(defn test-name
  "Store directory name, so no spaces. Options at their defaults are left out."
  [opts]
  (str/join "-"
            (cond-> ["pgdog" (name (:mode opts)) (name (:workload opts))]
              (= :append (:workload opts))
              (conj (name (:isolation opts)))

              (and (= :session-leak (:workload opts))
                   (not= session-leak/mechanisms (:session-mechanisms opts)))
              (conj (str/join "+" (map name (:session-mechanisms opts))))

              (and (= :append (:workload opts)) (= :session (:isolation-mode opts)))
              (conj "session-iso")

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
        wrap     (or (:wrap-generator workload) identity)]
    (merge tests/noop-test
           opts
           {:name      (test-name opts)
            :os        debian/os
            :db        db
            :client    (role/restrict-client :pgdog (:client workload))
            :nemesis   (:nemesis pkg)
            :checker   (checker/compose
                         {:perf       (checker/perf {:nemeses (:perf pkg)})
                          :stats      (checker/stats)
                          :exceptions (checker/unhandled-exceptions)
                          :timeline   (timeline/html)
                          :workload   (:checker workload)})
            :generator (wrap
                         (gen/phases
                           (->> (:generator workload)
                                (gen/stagger (/ (:rate opts)))
                                (gen/nemesis (:generator pkg))
                                (gen/time-limit (:time-limit opts)))
                           (gen/log "Healing cluster")
                           (gen/nemesis (:final-generator pkg))
                           (gen/log "Waiting for recovery")
                           (gen/sleep (:recovery-time opts))
                           (gen/clients (:final-generator workload))))})))

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

   [nil "--control" "Bypass PgDog: clients connect straight to Postgres (pooler mode only)"
    :default false]

   [nil "--isolation LEVEL" "serializable, repeatable-read or read-committed"
    :default :serializable
    :parse-fn keyword
    :validate [#{:serializable :repeatable-read :read-committed} "unknown isolation level"]]

   [nil "--isolation-mode MODE" "txn: SET TRANSACTION per txn; session: JDBC setTransactionIsolation"
    :default :txn
    :parse-fn keyword
    :validate [#{:txn :session} "must be txn or session"]]

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
  (-> opts
      (update-in [:ssh :private-key-path]
                 #(or % (str (System/getProperty "user.home") "/.ssh/id_jepsen")))
      pgdog-test))

(defn -main
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn  test-fn
                                         :opt-spec opt-spec})
                   (cli/serve-cmd))
            args))
