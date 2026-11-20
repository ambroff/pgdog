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
            [pgdog.jepsen.workload.append :as append]))

(def workloads
  {:append append/workload})

(def nemesis-faults
  "Faults accepted by --nemesis. kill/pause are per role; partition acts on
  any nodes (e.g. cutting PgDog off from a shard)."
  #{:kill-pgdog :pause-pgdog :kill-postgres :pause-postgres :partition})

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
                                  :partition {:targets [:one :majority]})))]
                      (remove nil?))]
    (if (seq pkgs)
      (nc/compose-packages pkgs)
      (nc/nemesis-package (assoc base :faults #{})))))

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
           ;; No spaces: this becomes the store/ directory name.
           {:name      (str/join "-"
                                 (cond-> ["pgdog" (name (:mode opts))
                                          (name (:workload opts))
                                          (name (:isolation opts))]
                                   (:control opts)       (conj "control")
                                   (seq (:nemesis opts)) (conj (str/join "+" (map name (:nemesis opts))))))
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
