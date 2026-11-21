(ns pgdog.jepsen.nemesis
  "PgDog-specific faults, as jepsen.nemesis.combined-style packages
  ({:nemesis, :generator, :final-generator, :perf}).

  - client partition: cut the Jepsen clients (which run on the control node)
    off from the server they talk to: PgDog, or Postgres with --control.
  - admin: RELOAD / RECONNECT on PgDog's admin database.
  - pool pause: PAUSE, then RESUME, on PgDog's admin database."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [control :as c]
                    [generator :as gen]
                    [nemesis :as n]
                    [role :as role]
                    [util :refer [meh]]]
            [jepsen.control.util :as cu]
            [jepsen.random :as rand]
            [next.jdbc :as j]
            [next.jdbc.result-set :as rs]
            [pgdog.jepsen.client :as pc]
            [pgdog.jepsen.db :as db]))

(defn client-targets
  "The nodes clients connect to, and the port: PgDog nodes, or with --control
  the Postgres node (every shard in sharded mode)."
  [test]
  (cond
    (not (:control test))     [(role/nodes test :pgdog) db/pgdog-port]
    (= :sharded (:mode test)) [(role/nodes test :postgres) db/pg-port]
    :else                     [(vec (take 1 (role/nodes test :postgres))) db/pg-port]))

(defn pick
  "Resolves a target spec (:one or :all) against `nodes`."
  [spec nodes]
  (case spec
    :one [(rand/nth nodes)]
    :all (vec nodes)))

;; ---------------------------------------------------------------------------
;; Client <-> server partition

(def rule-comment "jepsen-client-partition")

(defn control-ip
  "The control node's address, as seen by the node we're connected to."
  []
  (-> (c/exec :printenv :SSH_CLIENT)
      (str/split #"\s+")
      first))

(defn client-rules
  "iptables rule specs dropping traffic between `ip` and local `port`."
  [ip port]
  [[:INPUT  :-s ip :-p :tcp :--dport port]
   [:OUTPUT :-d ip :-p :tcp :--sport port]])

(defn iptables-rule!
  [action [chain & spec]]
  (apply c/exec :iptables :-w action chain
         (concat spec [:-m :comment :--comment rule-comment :-j :DROP])))

(defn heal-client-partition!
  "Deletes every client-partition rule on the current node. A rule may have
  been added more than once, or already flushed by another nemesis."
  [ip port]
  (doseq [rule (client-rules ip port)]
    (loop [n 0]
      (when (and (< n 16)
                 (try (iptables-rule! :-D rule) true
                      (catch Exception _ false)))
        (recur (inc n))))))

(defn client-partition-nemesis
  []
  (reify
    n/Reflection
    (fs [_] [:start-client-partition :stop-client-partition])

    n/Nemesis
    (setup! [this test]
      ;; An interrupted earlier run may have left rules behind.
      (let [[nodes port] (client-targets test)]
        (c/on-nodes test nodes
                    (fn [_ _]
                      (let [ip (control-ip)]
                        (c/su (heal-client-partition! ip port))))))
      this)

    (invoke! [_ test {:keys [f value] :as op}]
      (let [[nodes port] (client-targets test)]
        (case f
          :start-client-partition
          (let [targets (pick value nodes)]
            (c/on-nodes test targets
                        (fn [_ _]
                          (let [ip (control-ip)]
                            (c/su
                              (doseq [rule (client-rules ip port)]
                                (iptables-rule! :-I rule))))))
            (assoc op :value {:spec value, :nodes targets, :port port}))

          :stop-client-partition
          (do (c/on-nodes test nodes
                          (fn [_ _]
                            (let [ip (control-ip)]
                              (c/su (heal-client-partition! ip port)))))
              (assoc op :value :healed)))))

    (teardown! [_ test]
      (let [[nodes port] (client-targets test)]
        (meh (c/on-nodes test nodes
                         (fn [_ _]
                           (let [ip (control-ip)]
                             (c/su (heal-client-partition! ip port))))))))))

(defn client-partition-package
  [{:keys [interval]}]
  (let [start (fn [_ _] {:type :info, :f :start-client-partition,
                         :value (rand/nth [:one :all])})
        stop  {:type :info, :f :stop-client-partition, :value nil}]
    {:nemesis         (client-partition-nemesis)
     :generator       (->> (gen/flip-flop start (gen/repeat stop))
                           (gen/stagger interval))
     :final-generator stop
     :perf            #{{:name  "client-partition"
                         :start #{:start-client-partition}
                         :stop  #{:stop-client-partition}
                         :color "#C5A0E9"}}}))

;; ---------------------------------------------------------------------------
;; Admin database operations

(defn admin-on!
  "Runs `sql` on the admin database of every node in `nodes`, returning
  {node result}. A node that fails reports the error instead of aborting the
  others: a partitioned or killed PgDog shouldn't hide what happened on the
  rest."
  [nodes sql]
  (into (sorted-map)
        (map (fn [node]
               [node (try (pc/admin! node sql)
                          :ok
                          (catch Exception e
                            [:error (.getMessage e)]))]))
        nodes))

(defn admin-nemesis
  "Responds to `fs`, a subset of :reload, :reconnect, :pause-pool and
  :resume-pool, with :value :one or :all. With --control there is no PgDog in
  the client path, so ops are recorded but not executed."
  [fs]
  (reify
    n/Reflection
    (fs [_] fs)

    n/Nemesis
    (setup! [this test] this)

    (invoke! [_ test {:keys [f value] :as op}]
      (if (:control test)
        (assoc op :value :skipped-control)
        (let [nodes (role/nodes test :pgdog)
              ;; Resume everywhere, so a lost RESUME can't wedge a node.
              targets (if (= :resume-pool f) nodes (pick value nodes))
              sql (case f
                    :reload      "RELOAD"
                    :reconnect   "RECONNECT"
                    :pause-pool  "PAUSE"
                    :resume-pool "RESUME")]
          (assoc op :value (admin-on! targets sql)))))

    (teardown! [_ test]
      (when (and (not (:control test)) (some #{:resume-pool} fs))
        (admin-on! (role/nodes test :pgdog) "RESUME")))))

(defn admin-package
  "RELOAD and RECONNECT, as one-shot ops on one or all PgDog nodes."
  [{:keys [interval]}]
  {:nemesis   (admin-nemesis [:reload :reconnect])
   :generator (->> (fn [_ _] {:type  :info
                              :f     (rand/nth [:reload :reconnect])
                              :value (rand/nth [:one :all])})
                   (gen/stagger interval))
   :perf      #{{:name  "admin"
                 :fs    #{:reload :reconnect}
                 :start #{}
                 :stop  #{}
                 :color "#A0E9C5"}}})

(defn pool-pause-package
  "PAUSE then RESUME. PAUSE closes server connections and holds clients until
  RESUME; clients time out after the JDBC socketTimeout."
  [{:keys [interval]}]
  (let [start (fn [_ _] {:type :info, :f :pause-pool, :value (rand/nth [:one :all])})
        stop  {:type :info, :f :resume-pool, :value :all}]
    {:nemesis         (admin-nemesis [:pause-pool :resume-pool])
     :generator       (->> (gen/flip-flop start (gen/repeat stop))
                           (gen/stagger interval))
     :final-generator stop
     :perf            #{{:name  "pool-pause"
                         :start #{:pause-pool}
                         :stop  #{:resume-pool}
                         :color "#E9C5A0"}}}))

;; ---------------------------------------------------------------------------
;; Backend faults (Jepsen's PostgreSQL 12.3 analysis killed Postgres processes;
;; a pooler sees that as dead server connections)

(defn backends-sql
  "Selects the client backends the workload uses: PgDog's server connections,
  or the Jepsen clients themselves with --control. Both log in as db/user;
  with --control PgDog still runs and keeps idle connections, so only the
  clients' own sessions (application_name jepsen) are picked.
  With :one, a single backend, preferring one that is running a statement or
  is inside a transaction: most of the time PgDog's server connections sit
  idle in the pool, and killing those mostly tests reconnection."
  [test spec]
  (str "SELECT pid, coalesce(host(client_addr), ''), state, "
       "coalesce(backend_xid::text, ''), %s "
       "FROM pg_stat_activity "
       "WHERE usename = '" db/user "' AND datname = '" db/database "' "
       "AND backend_type = 'client backend' AND pid <> pg_backend_pid()"
       (when (:control test) " AND application_name = 'jepsen'")
       (when (= :one spec) " ORDER BY state = 'idle', random() LIMIT 1")))

(defn parse-backends [node rows]
  (mapv (fn [[pid addr state xid result]]
          (cond-> {:node        node
                   :pid         (parse-long pid)
                   :client-addr addr
                   :state       state}
            (seq xid)           (assoc :xid xid)
            (= "f" result)      (assoc :terminated? false)))
        rows))

(defn fault-node
  "The Postgres node a backend fault hits: a random shard in sharded mode,
  the only Postgres in pooler mode."
  [test]
  (rand/nth (role/nodes test :postgres)))

(defn terminate-backends!
  "pg_terminate_backend on one or all of the workload's backends on `pg`.
  Each gets FATAL 57P01 and its connection closes."
  [test pg spec]
  (do
    (c/on-nodes test [pg]
                (fn [_ node]
                  (parse-backends
                    node
                    (db/psql-rows! db/database
                                   (format (backends-sql test spec)
                                           "pg_terminate_backend(pid)")))))))

(defn kill-backend!
  "SIGKILLs one of the workload's backends on `pg`. The postmaster then
  treats it as a crash: it kills every other backend (they get 57P02 or just
  EOF), runs crash recovery and accepts connections again."
  [test pg]
  (do
    (c/on-nodes test [pg]
                (fn [_ node]
                  (let [backends (parse-backends
                                   node
                                   (db/psql-rows! db/database
                                                  (format (backends-sql test :one) "'t'")))]
                    (c/su (doseq [{:keys [pid]} backends]
                            (meh (c/exec :kill :-9 pid))))
                    backends)))))

(defn backend-nemesis
  "Ops:
    {:f :terminate-backend, :value :one|:all}
    {:f :kill-backend}
    {:f :restart-postgres, :value down-ms}   pg_ctl stop -m immediate, start
  Each completes with the backends it hit, so the backend checker can match
  client errors to them."
  []
  (reify
    n/Reflection
    (fs [_] [:terminate-backend :kill-backend :restart-postgres :start-postgres])

    n/Nemesis
    (setup! [this test] this)

    (invoke! [_ test {:keys [f value] :as op}]
      ;; terminate/kill look up backends with psql, which Postgres refuses
      ;; while it is still in crash recovery from a previous fault. That hits
      ;; nothing; record it rather than crash the op.
      (let [pg (fault-node test)]
        (letfn [(skipped [e]
                  (assoc op :value {:spec value, :node pg, :backends [],
                                    :skipped (.getMessage ^Throwable e)}))]
          (case f
            :terminate-backend
            (try
              (let [hit (terminate-backends! test pg value)]
                (assoc op :value {:spec value, :node pg, :backends (vec (mapcat val hit))}))
              (catch Exception e (skipped e)))

            :kill-backend
            (try
              (let [hit (kill-backend! test pg)]
                (assoc op :value {:node pg, :backends (vec (mapcat val hit))}))
              (catch Exception e (skipped e)))

            :restart-postgres
            (do (c/on-nodes test [pg] (fn [t _] (db/restart-immediate! t value)))
                (assoc op :value {:down-ms value, :node pg}))

            ;; Final heal: make sure every Postgres is up even if a restart
            ;; failed halfway (stopped, then the start errored).
            :start-postgres
            (do (c/on-nodes test (role/nodes test :postgres)
                            (fn [t _] (db/ensure-started! t)))
                (assoc op :value :started))))))

    (teardown! [_ test])))

(def backend-faults
  "--nemesis names for backend faults, and the op each one generates."
  {:terminate-backend (fn [] {:f :terminate-backend, :value (rand/nth [:one :one :all])})
   :kill-backend      (fn [] {:f :kill-backend, :value nil})
   :restart-postgres  (fn [] {:f :restart-postgres, :value (rand/nth [0 2000 7000])})})

(defn backend-package
  "Backend faults, one-shot ops staggered by the nemesis interval. `faults`
  is a subset of the keys of `backend-faults`. These act on the single
  Postgres (pooler mode); with --control they hit the clients' own backends,
  which is the baseline."
  [{:keys [interval faults]}]
  (let [fs (vec (filter (set faults) (keys backend-faults)))]
    {:nemesis         (backend-nemesis)
     :generator       (->> (fn [_ _] (assoc ((backend-faults (rand/nth fs))) :type :info))
                           (gen/stagger interval))
     :final-generator {:type :info, :f :start-postgres, :value nil}
     :perf      #{{:name  "backend"
                   :fs    #{:terminate-backend :kill-backend :restart-postgres :start-postgres}
                   :start #{}
                   :stop  #{}
                   :color "#E9A0A0"}}}))

;; ---------------------------------------------------------------------------
;; Pool stats: does every pool drain once the workload is done?

(defn ->long [v] (when (some? v) (long (bigdec (str v)))))

(defn pool-rows
  "SHOW POOLS rows for the workload's database and user on `node`."
  [node]
  (->> (pc/admin! node "SHOW POOLS")
       (map (fn [row] (into {} (map (fn [[k v]] [(keyword (name k)) v])) row)))
       (filter #(and (= db/database (:database %)) (= db/user (:user %))))
       (mapv (fn [r] (-> (select-keys r [:addr :shard :cl_waiting :sv_idle :sv_active
                                         :sv_idle_xact :sv_total :errors
                                         :force_closed :healthy])
                         (update :cl_waiting ->long) (update :sv_idle ->long)
                         (update :sv_active ->long) (update :sv_idle_xact ->long)
                         (update :sv_total ->long) (update :errors ->long)
                         (update :force_closed ->long))))))

(defn drained? [rows]
  (every? #(and (zero? (:sv_active %)) (zero? (:cl_waiting %))) rows))

(defn pool-stats!
  "Polls SHOW POOLS on `node` for up to `timeout-ms` until no server
  connection is checked out and no client is waiting. Clients are idle by
  now, so in transaction mode every server should be back in the pool. A
  checked-out count that never drains is a leaked connection slot: the pool
  has permanently lost capacity."
  [node timeout-ms]
  (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
    (loop []
      (let [rows (try (pool-rows node)
                      (catch Exception e [:error (.getMessage e)]))]
        (cond
          (and (vector? rows) (not= :error (first rows)) (drained? rows))
          {:drained? true, :pools rows}

          (< (System/currentTimeMillis) deadline)
          (do (Thread/sleep 500) (recur))

          :else
          {:drained? false, :pools rows})))))

(defn pool-stats-nemesis
  "{:f :pool-stats} reports pool-stats! for every PgDog node."
  []
  (reify
    n/Reflection
    (fs [_] [:pool-stats])

    n/Nemesis
    (setup! [this test] this)

    (invoke! [_ test op]
      (if (:control test)
        (assoc op :value :skipped-control)
        (assoc op :value (into (sorted-map)
                               (map (fn [node] [node (pool-stats! node (* 1000 (:drain-timeout test 15)))]))
                               (role/nodes test :pgdog)))))

    (teardown! [_ test])))

;; ---------------------------------------------------------------------------
;; Direct reads of every shard (sharded mode): ground truth for placement

(defn direct-conn
  "A JDBC connection straight to Postgres on `node`, bypassing PgDog."
  [node]
  (j/get-connection {:dbtype         "postgresql"
                     :host           node
                     :port           db/pg-port
                     :dbname         db/database
                     :user           db/user
                     :password       db/password
                     :connectTimeout 5
                     :socketTimeout  60}))

(defn xacts
  "Committed plus rolled-back txns in the jepsen database, from
  pg_stat_database."
  [conn]
  (some-> (j/execute-one! conn [(str "SELECT (xact_commit + xact_rollback)::bigint AS n "
                                     "FROM pg_stat_database WHERE datname = ?")
                                db/database])
          :n))

(defn shard-dump!
  "{node {:shard s, :xacts n, :rows [[k v] ...]}} over every Postgres node,
  read directly. With rows? false, only the txn counts. A node that can't be
  read reports the error instead."
  [test rows?]
  (into (sorted-map)
        (for [node (role/nodes test :postgres)]
          [node
           (try
             (with-open [^java.sql.Connection conn (direct-conn node)]
               (cond-> {:shard (db/shard-index test node)
                        :xacts (xacts conn)}
                 rows?
                 (assoc :rows (vec (for [t   (db/dump-tables test)
                                         row (j/execute! conn [(str "SELECT k, "
                                                                    (if (= db/bank-table t) "balance" "v")
                                                                    " AS v FROM " t " ORDER BY k")]
                                                         {:builder-fn rs/as-unqualified-maps})]
                                     [(:k row) (:v row)])))))
             (catch Exception e
               {:error (.getMessage e)}))])))

(defn shard-dump-nemesis
  "{:f :xact-stats} records each shard's txn count; {:f :shard-dump} also
  reads every row. Sharded mode only; see client/placement-checker."
  []
  (reify
    n/Reflection
    (fs [_] [:xact-stats :shard-dump])

    n/Nemesis
    (setup! [this test] this)

    (invoke! [_ test {:keys [f] :as op}]
      (assoc op :value (shard-dump! test (= :shard-dump f))))

    (teardown! [_ test])))
