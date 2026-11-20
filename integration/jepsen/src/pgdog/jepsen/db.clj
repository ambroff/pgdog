(ns pgdog.jepsen.db
  "Installs and runs Postgres and PgDog on Jepsen nodes. The test's :roles map
  decides which nodes run what:

    {:pgdog    [\"n1\" \"n2\"]
     :postgres [\"n3\" ...]}

  In :pooler mode PgDog proxies to a single Postgres. In :sharded mode every
  :postgres node is one shard."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info]]
            [jepsen [control :as c]
                    [db :as db]
                    [role :as role]
                    [util :refer [meh]]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [pgdog.jepsen.shard :as shard]))

(def user "jepsen")
(def password "jepsen")
(def database "jepsen")
(def pg-port 5432)
(def pgdog-port 6432)
;; PgDog's admin database. The admin user doesn't need to be in users.toml.
(def admin-database "admin")
(def admin-user "admin")

;; ---------------------------------------------------------------------------
;; Postgres

(defn pg-unit [test] (str "postgresql@" (:pg-version test) "-main"))
(defn pg-conf-dir [test] (str "/etc/postgresql/" (:pg-version test) "/main"))
(defn pg-log [test]
  (str "/var/log/postgresql/postgresql-" (:pg-version test) "-main.log"))

(defn ensure-started!
  "Starts Postgres if it isn't running and waits for its port. Retries: right
  after an immediate stop, systemd may not have noticed the postmaster exit
  yet and treat start as a no-op."
  [test]
  (c/su
    (loop [tries 5]
      (meh (c/exec :systemctl :reset-failed (pg-unit test)))
      (meh (c/exec :systemctl :start (pg-unit test)))
      (let [up? (try (cu/await-tcp-port pg-port {:timeout 15000}) true
                     (catch Exception _ false))]
        (when-not up?
          (if (pos? tries)
            (do (Thread/sleep 1000) (recur (dec tries)))
            (throw (IllegalStateException. "Postgres did not come back up"))))))))

(defn restart-immediate!
  "pg_ctl stop -m immediate (every backend is killed without a checkpoint),
  waits `down-ms`, then starts Postgres again, which runs crash recovery.
  Stopping outside systemd leaves the unit failed; start clears that."
  [test down-ms]
  (c/su
    (c/exec :pg_ctlcluster :--skip-systemctl-redirect (:pg-version test) "main"
            :stop :-m :immediate)
    (when (pos? down-ms) (Thread/sleep (long down-ms)))
    (ensure-started! test)))

(defn psql!
  "Runs SQL as the postgres superuser against the given database."
  ([sql] (psql! "postgres" sql))
  ([db sql]
   (c/cd "/tmp"
     (c/sudo "postgres"
       (c/exec :psql :-v "ON_ERROR_STOP=1" :-d db :-c sql)))))

(defn install-postgres!
  [test]
  (let [pkg (str "postgresql-" (:pg-version test))]
    (when-not (cu/exists? (str "/usr/lib/postgresql/" (:pg-version test)))
      (info "installing" pkg "from PGDG")
      (debian/install [:postgresql-common])
      (c/exec "/usr/share/postgresql-common/pgdg/apt.postgresql.org.sh" :-y)
      (debian/install [pkg]))))

(defn psql-rows!
  "Runs a query as the postgres superuser and returns its rows as vectors of
  strings (psql unaligned, tab-separated)."
  [db sql]
  (let [out (c/cd "/tmp"
              (c/sudo "postgres"
                (c/exec :psql :-v "ON_ERROR_STOP=1" :-d db :-At :-F "\t" :-c sql)))]
    (->> (str/split-lines out)
         (remove str/blank?)
         (mapv #(str/split % #"\t" -1)))))

(defn append-tables
  "Table names for the append workload. One table keeps M1's name, `append`;
  --tables N spreads keys over append_0 .. append_<N-1>."
  [test]
  (let [n (:tables test 1)]
    (if (= 1 n)
      ["append"]
      (mapv #(str "append_" %) (range n)))))

(def probe-table
  "Sharded mode: one row per shard, at the shard's anchor key (see
  shard/anchor). Probe statements select from it by k, so PgDog routes them
  to the txn's shard like any other keyed statement."
  "jepsen_probe")

(defn shard-index
  "Index of a Postgres node among the shards, in config order."
  [test node]
  (.indexOf ^java.util.List (role/nodes test :postgres) node))

(defn schema
  "DDL for every workload. Kept here (not in clients) so it is applied directly
  on each Postgres node, independent of PgDog's DDL routing. `sk` is a
  secondary key with no index (always equal to k), for reads that must scan
  the table (--read-by sk). In sharded mode `node` gets the probe row of its
  own shard only, where PgDog would have put it."
  [test node]
  (concat
    (for [t (append-tables test)]
      (str "CREATE TABLE IF NOT EXISTS " t
           " (k bigint PRIMARY KEY, sk bigint NOT NULL, v text NOT NULL)"))
    [(str "CREATE TABLE IF NOT EXISTS " probe-table " (k bigint PRIMARY KEY)")]
    (when (= :sharded (:mode test))
      [(str "INSERT INTO " probe-table " VALUES ("
            (shard/anchor test (shard-index test node)) ")")])))

(defn configure-postgres!
  [test node]
  (c/exec :mkdir :-p (str (pg-conf-dir test) "/conf.d"))
  (cu/write-file!
    (str/join "\n"
              ["listen_addresses = '*'"
               "max_connections = 500"
               ;; Required for PgDog two-phase commit.
               "max_prepared_transactions = 500"
               ;; Logical replication is needed for resharding tests later.
               "wal_level = logical"
               "log_line_prefix = '%m [%p] %q%u@%d '"
               ;; Lets a client see which node a statement ran on.
               (str "cluster_name = '" node "'")
               ""])
    (str (pg-conf-dir test) "/conf.d/jepsen.conf"))
  (let [hba (str (pg-conf-dir test) "/pg_hba.conf")]
    (when-not (re-find #"jepsen" (c/exec :cat hba))
      (c/exec :echo "host all all 0.0.0.0/0 scram-sha-256 # jepsen"
              (c/lit ">>") hba)))
  (c/exec :systemctl :restart (pg-unit test))
  (cu/await-tcp-port pg-port {:timeout 60000}))

(defn create-database!
  [test node]
  (psql! (str "DROP DATABASE IF EXISTS " database " WITH (FORCE)"))
  (psql! (str "DROP ROLE IF EXISTS " user))
  (psql! (str "CREATE ROLE " user " LOGIN PASSWORD '" password "'"))
  (psql! (str "CREATE DATABASE " database " OWNER " user))
  (doseq [ddl (schema test node)]
    (psql! database (str "SET ROLE " user "; " ddl))))

(defrecord Postgres []
  db/DB
  (setup! [_ test node]
    (c/su
      (install-postgres! test)
      (configure-postgres! test node)
      (create-database! test node)))

  (teardown! [_ test node]
    (c/su
      (when (cu/exists? (str "/usr/lib/postgresql/" (:pg-version test)))
        (meh (c/exec :systemctl :start (pg-unit test)))
        (meh (psql! (str "DROP DATABASE IF EXISTS " database " WITH (FORCE)"))))))

  db/Kill
  (kill! [_ test node]
    (c/su (meh (c/exec :systemctl :kill :-s :KILL (pg-unit test)))))
  (start! [_ test node]
    (c/su (c/exec :systemctl :start (pg-unit test))))

  db/Pause
  (pause! [_ test node]
    (c/su (c/exec :systemctl :kill :-s :STOP (pg-unit test))))
  (resume! [_ test node]
    (c/su (c/exec :systemctl :kill :-s :CONT (pg-unit test))))

  db/LogFiles
  (log-files [_ test node]
    [(pg-log test)]))

;; ---------------------------------------------------------------------------
;; PgDog

(def pgdog-dir "/opt/pgdog")
(def pgdog-bin (str pgdog-dir "/pgdog"))
(def pgdog-log (str pgdog-dir "/pgdog.log"))
(def pgdog-pid (str pgdog-dir "/pgdog.pid"))
(def pgdog-wal (str pgdog-dir "/wal"))

(defn toml-str [s] (str "\"" s "\""))

(defn pgdog-toml
  "Renders pgdog.toml for the test's mode."
  [test]
  (let [pg-nodes (role/nodes test :postgres)
        general  (cond-> ["[general]"
                          "host = \"0.0.0.0\""
                          (str "port = " pgdog-port)
                          (str "default_pool_size = " (:pool-size test))
                          (str "pooler_mode = " (toml-str (name (:pooler-mode test))))
                          (str "query_parser = " (toml-str (name (:query-parser test))))
                          (str "two_phase_commit_wal_dir = " (toml-str pgdog-wal))]
                   (:two-pc test)
                   (conj "two_phase_commit = true")

                   (some? (:two-pc-auto test))
                   (conj (str "two_phase_commit_auto = " (:two-pc-auto test))))
        dbs      (case (:mode test)
                   :pooler
                   [["[[databases]]"
                     (str "name = " (toml-str database))
                     (str "host = " (toml-str (first pg-nodes)))
                     (str "port = " pg-port)
                     (str "database_name = " (toml-str database))]]

                   :sharded
                   (map-indexed
                     (fn [shard node]
                       ["[[databases]]"
                        (str "name = " (toml-str database))
                        (str "host = " (toml-str node))
                        (str "port = " pg-port)
                        (str "database_name = " (toml-str database))
                        (str "shard = " shard)])
                     pg-nodes))
        ;; Every table is sharded on k with the same placement (see
        ;; pgdog.jepsen.shard). Mapping entries attach to the
        ;; [[sharded_tables]] entry just above them.
        sharding (when (= :sharded (:mode test))
                   (for [t (conj (append-tables test) probe-table)]
                     (concat ["[[sharded_tables]]"
                              (str "database = " (toml-str database))
                              (str "name = " (toml-str t))
                              "column = \"k\""
                              "data_type = \"bigint\""]
                             (shard/mapping-toml test))))
        admin    ["[admin]"
                  (str "name = " (toml-str admin-database))
                  (str "user = " (toml-str admin-user))
                  (str "password = " (toml-str password))]]
    (->> (concat [general] dbs sharding [admin])
         (map #(str/join "\n" %))
         (str/join "\n\n"))))

(defn users-toml
  []
  (str/join "\n" ["[[users]]"
                  (str "name = " (toml-str user))
                  (str "database = " (toml-str database))
                  (str "password = " (toml-str password))
                  ""]))

(defn node-id
  "Stable NODE_ID for a PgDog node, e.g. pgdog-1. :node-id false leaves it unset."
  [test node]
  (when (:node-id test true)
    (str "pgdog-" (inc (.indexOf ^java.util.List (role/nodes test :pgdog) node)))))

(defn start-pgdog!
  [test node]
  (c/su
    (cu/start-daemon!
      {:logfile pgdog-log
       :pidfile pgdog-pid
       :chdir   pgdog-dir
       :env     (cond-> {:RUST_LOG "info"}
                  (node-id test node) (assoc :NODE_ID (node-id test node)))}
      pgdog-bin
      :--config (str pgdog-dir "/pgdog.toml")
      :--users  (str pgdog-dir "/users.toml"))))

(defrecord PgDog []
  db/DB
  (setup! [_ test node]
    (let [bin (io/file (:pgdog-bin test))]
      (when-not (.exists bin)
        (throw (IllegalStateException.
                 (str "PgDog binary not found at " (.getCanonicalPath bin)
                      "; run cargo build --release")))))
    (c/su
      (c/exec :mkdir :-p pgdog-dir pgdog-wal)
      (c/upload (:pgdog-bin test) pgdog-bin)
      (c/exec :chmod :+x pgdog-bin)
      (cu/write-file! (pgdog-toml test) (str pgdog-dir "/pgdog.toml"))
      (cu/write-file! (users-toml) (str pgdog-dir "/users.toml")))
    (start-pgdog! test node)
    (cu/await-tcp-port pgdog-port {:timeout 60000}))

  (teardown! [_ test node]
    (c/su
      (cu/stop-daemon! "pgdog" pgdog-pid)
      (c/exec :rm :-rf pgdog-dir)))

  db/Kill
  (kill! [_ test node]
    (c/su (cu/stop-daemon! "pgdog" pgdog-pid)))
  (start! [_ test node]
    (start-pgdog! test node))

  db/Pause
  (pause! [_ test node]
    (c/su (cu/signal! "pgdog" :STOP)))
  (resume! [_ test node]
    (c/su (cu/signal! "pgdog" :CONT)))

  db/LogFiles
  (log-files [_ test node]
    {pgdog-log                       "pgdog.log"
     (str pgdog-dir "/pgdog.toml")   "pgdog.toml"}))

(defn db
  "Composite DB: Postgres on :postgres nodes, PgDog on :pgdog nodes. PgDog
  setup waits for every Postgres node."
  []
  (role/db {:postgres (Postgres.)
            :pgdog    {:db (PgDog.) :deps [:postgres]}}))
