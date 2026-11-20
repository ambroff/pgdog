(ns pgdog.jepsen.client
  "JDBC plumbing shared by every workload: connections through PgDog (or
  straight to Postgres with --control), transactions, and the error
  classification that decides whether a failed op is :fail (definitely did not
  happen) or :info (may or may not have happened)."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen.checker :as checker]
            [jepsen.history :as h]
            [jepsen.role :as role]
            [next.jdbc :as j]
            [pgdog.jepsen.db :as db])
  (:import (java.sql Connection SQLException)
           (org.postgresql.util PSQLException)))

(defn target
  "Where a client on `node` should connect: [host port]. In --control mode,
  bypass PgDog and connect to Postgres directly (pooler mode only)."
  [test node]
  (if (:control test)
    [(first (role/nodes test :postgres)) db/pg-port]
    [node db/pgdog-port]))

(defn open
  "Opens a JDBC connection to `node` (a PgDog node, unless --control). `extra`
  is merged into the connection properties, e.g. {:options \"-c x=y\"} to
  send startup parameters."
  (^Connection [test node] (open test node {}))
  (^Connection [test node extra]
   (let [[host port] (target test node)]
     (j/get-connection
       (merge
         {:dbtype           "postgresql"
          :host             host
          :port             port
          :dbname           db/database
          :user             db/user
          :password         db/password
          ;; 0 = unnamed statements only; >0 = named server-side prepared
          ;; statements after N uses (exercises PgDog's statement rewriting).
          :prepareThreshold (:prepare-threshold test)
          :preferQueryMode  (name (:query-mode test))
          :connectTimeout   5
          :socketTimeout    10
          :tcpKeepAlive     true
          :ApplicationName  "jepsen"}
         extra)))))

(defn admin!
  "Runs one command (e.g. RELOAD) on a PgDog node's admin database. The admin
  database only speaks the simple query protocol."
  [node sql]
  (with-open [conn (j/get-connection
                     {:dbtype          "postgresql"
                      :host            node
                      :port            db/pgdog-port
                      :dbname          db/admin-database
                      :user            db/admin-user
                      :password        db/password
                      :preferQueryMode "simple"
                      :connectTimeout  5
                      :socketTimeout   10})]
    (j/execute! conn [sql])))

(defn close! [^Connection conn]
  (when conn
    (try (.close conn) (catch Throwable _))))

(def isolation-sql
  {:serializable    "SERIALIZABLE"
   :repeatable-read "REPEATABLE READ"
   :read-committed  "READ COMMITTED"})

(def isolation-jdbc
  {:serializable    Connection/TRANSACTION_SERIALIZABLE
   :repeatable-read Connection/TRANSACTION_REPEATABLE_READ
   :read-committed  Connection/TRANSACTION_READ_COMMITTED})

(defn sql-state [^Throwable e]
  (when (instance? SQLException e)
    (.getSQLState ^SQLException e)))

(defn severity
  "Severity of a server-sent error (ERROR, FATAL, ...), or nil for errors the
  driver made up. A FATAL means the server side closed the session; PgDog
  sends its own FATAL 58000 when it loses its server connection, where a
  direct pgjdbc connection would report 08006."
  [^Throwable e]
  (when (instance? PSQLException e)
    (some-> ^PSQLException e .getServerErrorMessage .getSeverity)))

(defn connection-error?
  "True when the error means the connection itself is gone or unusable."
  [^Throwable e]
  (let [state (sql-state e)]
    (or (nil? state)
        (str/starts-with? state "08")        ; connection exception
        (= "57P01" state)                    ; admin shutdown
        (= "57P02" state)                    ; crash shutdown
        (= "57P03" state))))                 ; cannot connect now

(defn definite-failure?
  "Did this error definitely abort the transaction, given the phase it happened
  in (:exec before COMMIT was sent, :commit during COMMIT)?

  Postgres guarantees that an error response (as opposed to a lost connection)
  means the transaction did not commit. With --strict-commit-errors we hold
  PgDog to the same rule for every error, including during COMMIT (hypothesis
  H4 in PLAN.org): an error on COMMIT that later turns out committed shows up
  in Elle as an aborted read (G1a)."
  [test phase ^Throwable e]
  (and (instance? PSQLException e)
       (not (connection-error? e))
       (or (= :exec phase)
           (:strict-commit-errors test)
           (str/starts-with? (sql-state e) "40"))))

(defn error-type
  "Short keyword describing an error, for op :error fields."
  [^Throwable e]
  (case (sql-state e)
    "40001" :serialization-failure
    "40P01" :deadlock
    "25P02" :in-failed-txn
    nil     (keyword (.getSimpleName (class e)))
    (keyword (str "sqlstate-" (sql-state e)))))

(defn txn-error-type
  "Completion type for an explicit transaction that threw in `phase`.

  Before COMMIT is sent (:connect, :exec) the transaction cannot have
  committed, whatever the error, including a lost connection: Postgres only
  commits on COMMIT, and a transparent pooler must roll back a transaction
  whose client or server went away. So these are definite failures. If PgDog
  ever replays or commits such a transaction, Elle sees an aborted read (G1a)
  or a lost/duplicated append. During COMMIT only an error response is
  definite, as in `definite-failure?`."
  [test phase ^Throwable e]
  (if (or (#{:connect :exec} phase) (definite-failure? test phase e))
    :fail
    :info))

(defn run-txn!
  "Runs (f conn) inside a transaction at the test's isolation level, and
  completes `op`. Returns the completed op. Reconnects lazily: on a connection
  error `conn-atom` is reset to nil and the next call opens a new one.

  --isolation-mode :txn issues SET TRANSACTION ISOLATION LEVEL as the first
  statement of every transaction. :session uses JDBC's setTransactionIsolation,
  which pgjdbc sends as SET SESSION CHARACTERISTICS: through a transaction
  pooler, that only works if PgDog carries session state across server
  connections (claim C2).

  Every transaction also records the server backend it ran on, at its start
  (:backend-pid) and just before COMMIT (:backend-pid-end). Through a
  transaction pooler both must be the same backend; see `backend-checker`.
  Failed ops keep :backend-pid when it was read, so an error can be matched to
  the backend a nemesis terminated."
  [test node conn-atom op f]
  (let [phase (volatile! :connect)
        pid   (volatile! nil)]
    (try
      (let [^Connection conn (or @conn-atom (reset! conn-atom (open test node)))
            iso (:isolation test)]
        (vreset! phase :exec)
        (when (= :session (:isolation-mode test))
          (.setTransactionIsolation conn (isolation-jdbc iso)))
        (.setAutoCommit conn false)
        (when (= :txn (:isolation-mode test))
          (j/execute! conn [(str "SET TRANSACTION ISOLATION LEVEL "
                                 (isolation-sql iso))]))
        ;; Record the level the server actually runs this txn at, so a
        ;; txn that silently ran at a weaker level shows up even when Elle
        ;; finds no anomaly (see isolation-checker).
        (let [row   (j/execute-one!
                      conn [(str "SELECT current_setting('transaction_isolation') AS iso, "
                                 "pg_backend_pid() AS pid")])
              _     (vreset! pid (:pid row))
              value (f conn)
              end   (:pid (j/execute-one! conn ["SELECT pg_backend_pid() AS pid"]))]
          (vreset! phase :commit)
          (.commit conn)
          (assoc op :type :ok, :value value, :isolation-seen (:iso row)
                 :backend-pid @pid, :backend-pid-end end)))
      (catch Throwable e
        (let [conn @conn-atom]
          ;; A workload abort (ex-info) is not a lost connection: roll back
          ;; and keep using the connection.
          (if (or (nil? conn)
                  (and (connection-error? e)
                       (not (instance? clojure.lang.ExceptionInfo e))))
            (do (close! conn) (reset! conn-atom nil))
            (try (.rollback ^Connection conn)
                 (catch Throwable _
                   (close! conn) (reset! conn-atom nil)))))
        (cond-> (assoc op
                       :type  (txn-error-type test @phase e)
                       :error [@phase (error-type e) (.getMessage e)])
          @pid         (assoc :backend-pid @pid)
          (severity e) (assoc :error-severity (severity e)))))))

(defn nanos->s [t] (when t (/ (Math/round (/ t 1e7)) 100.0)))

(defn isolation-checker
  "Checks that every ok transaction ran at the isolation level the test asked
  for, as reported by the server inside the transaction (:isolation-seen).
  :first-wrong-s and :wrong-per-s say how long a run needs to be to catch a
  wrong level, for sizing regression tests."
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [want  (str/lower-case (isolation-sql (:isolation test)))
            oks   (filter #(and (= :ok (:type %)) (contains? % :isolation-seen)) history)
            wrong (remove #(= want (:isolation-seen %)) oks)
            span  (some-> (last oks) :time nanos->s)]
        {:valid?        (empty? wrong)
         :expected      want
         :ok-count      (count oks)
         :seen          (frequencies (map :isolation-seen oks))
         :wrong-count   (count wrong)
         :wrong-frac    (when (seq oks) (double (/ (count wrong) (count oks))))
         :first-wrong-s (some-> (first wrong) :time nanos->s)
         :wrong-per-s   (when (and span (pos? span)) (/ (count wrong) span))
         :examples      (vec (take 10 wrong))}))))

(def termination-error
  "Message Postgres sends with SQLSTATE 57P01 to a backend's client when
  pg_terminate_backend kills it (or on a fast shutdown)."
  #"terminating connection due to administrator command")

(defn connection-loss?
  "Did this txn fail because a connection (client<->PgDog or PgDog<->Postgres)
  went away? PgDog reports a dead server connection as its own FATAL
  SQLSTATE 58000 (\"net: io: ...\"), not as Postgres' 57P01/57P02."
  [{:keys [error]}]
  (let [[_ type msg] error
        state (some->> type name (re-find #"^sqlstate-(.*)$") second)]
    (or (and (string? msg) (re-find termination-error msg))
        (= "58000" state)
        (and state (or (str/starts-with? state "08") (str/starts-with? state "57P")))
        ;; Driver-level errors (no SQLSTATE): socket closed, EOF, timeouts.
        (and type (nil? state) (not (#{:serialization-failure :deadlock :in-failed-txn
                                       :ExceptionInfo :neighbor} type))))))

(defn nemesis-pairs
  "[invocation completion] for every nemesis op that completed. Nemesis
  invocations and completions are both typed :info, and h/completion maps
  either one to the other, so the invocation is the one with the lower index."
  [history]
  (->> history
       (filter #(= :nemesis (:process %)))
       (keep (fn [op]
               (let [c (h/completion history op)]
                 (when (and c (< (:index op) (:index c)))
                   [op c]))))))

(defn nemesis-windows
  "[f invoke-time complete-time value] for every completed nemesis op."
  [history]
  (mapv (fn [[i c]] [(:f c) (:time i) (:time c) (:value c)])
        (nemesis-pairs history)))

(defn terminated-pids
  "{pid earliest-invoke-time} for every backend the backend nemesis
  terminated or SIGKILLed. The kill happens between the nemesis op's invoke
  and completion, so the invoke time is the earliest it can have happened."
  [windows]
  (reduce (fn [m [f t0 _ value]]
            (if (#{:terminate-backend :kill-backend} f)
              (reduce (fn [m {:keys [pid]}]
                        (if pid (update m pid #(if % (min % t0) t0)) m))
                      m (:backends value))
              m))
          {} windows))

(def connection-faults
  "Nemesis ops that can kill or cut connections, now or (for dead idle
  server connections handed out later) at any later time."
  #{:terminate-backend :kill-backend :restart-postgres :kill :pause :start-partition
    :start-client-partition :reconnect :reload :pause-pool})

(defn attribute
  "Why did this connection-loss error happen?
  - :pid-terminated: its backend was hit by the nemesis before the txn ended.
  - :other-fault: a nemesis op other than terminate-backend overlapped the
    txn (crash restarts kill every backend; PgDog kills and partitions cut
    connections).
  - :before-first-statement: the txn never learned its backend (the first
    statement on the server connection failed, e.g. a dead idle connection
    was handed to it), and some connection fault happened before it ended.
  - :unexplained: none of the above. Either another client's backend death
    reached this txn, or a connection died with no fault to explain it."
  [windows killed history op]
  (let [inv (h/invocation history op)
        t0  (:time inv)
        t1  (:time op)
        pid (:backend-pid op)
        overlapping (filter (fn [[_ s e]] (and (<= s t1) (<= t0 e))) windows)]
    (cond
      (and pid (some-> (killed pid) (<= t1)))
      :pid-terminated

      (some (comp (complement #{:terminate-backend :pool-stats}) first) overlapping)
      :other-fault

      (and (nil? pid)
           (some (fn [[f s]] (and (connection-faults f) (<= s t1))) windows))
      :before-first-statement

      :else
      :unexplained)))

(defn backend-checker
  "Checks what PgDog does with server backends, using the pids that
  `run-txn!` records:

  - :split-txns: ok transactions whose statements did not all run on one
    backend (:backend-pid at the start differs from :backend-pid-end). A
    transaction-mode pooler must keep a transaction on one server connection;
    a different backend means PgDog moved or replayed part of it.
  - :unexplained: transactions that failed with a lost connection although
    their backend was never terminated and no other fault overlapped them:
    another client's backend death reached this client.
  - :attribution counts every connection-loss error by cause (see
    `attribute`)."
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [txns    (filter #(and (= :txn (:f %)) (integer? (:process %))
                                  (not= :invoke (:type %)))
                            history)
            split   (filter #(and (= :ok (:type %))
                                  (:backend-pid-end %)
                                  (not= (:backend-pid %) (:backend-pid-end %)))
                            txns)
            windows (nemesis-windows history)
            killed  (terminated-pids windows)
            lost    (->> txns
                         (remove #(= :ok (:type %)))
                         (filter connection-loss?)
                         (map #(assoc % :attribution (attribute windows killed history %))))
            unexplained (filter #(= :unexplained (:attribution %)) lost)
            finals  (filter :final? txns)]
        {:valid?            (and (empty? split) (empty? unexplained)
                                 (every? #(= :ok (:type %)) finals))
         ;; Liveness after healing: the final reads go through PgDog, so a
         ;; failed one means the pool did not recover.
         :final-reads       (frequencies (map :type finals))
         :ok-with-pids      (count (filter #(and (= :ok (:type %)) (:backend-pid-end %)) txns))
         :distinct-backends (count (distinct (keep :backend-pid txns)))
         :terminated-pids   (count killed)
         :errors-by-type    (frequencies (map (comp second :error)
                                              (remove #(= :ok (:type %)) txns)))
         :connection-loss   (count lost)
         :attribution       (frequencies (map :attribution lost))
         :lost-by-type      (frequencies (map (juxt :type (comp first :error)) lost))
         :split-count       (count split)
         :split-txns        (vec (take 10 split))
         :unexplained-count (count unexplained)
         :unexplained       (vec (take 10 unexplained))}))))

(defn pool-checker
  "After the workload, every PgDog pool must drain: no server connection
  still checked out and no client waiting (the :pool-stats op at the end of
  the test). A checked-out count that never drains is a leaked slot: the pool
  has lost capacity for good. Unknown if the stats couldn't be read."
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [stats (->> (nemesis-pairs history)
                       (map second)
                       (filter #(= :pool-stats (:f %)))
                       last :value)]
        (cond
          ;; In session mode every connected client holds a server
          ;; connection, and the clients are still connected here.
          (= :session (:pooler-mode test))
          {:valid? true, :note "skipped: session pooler mode", :stats stats}

          (not (map? stats))
          {:valid? (if (= :skipped-control stats) true :unknown), :stats stats}

          :else
          (let [undrained (into (sorted-map) (remove (comp :drained? val)) stats)
                ;; A PgDog restart resets its pool counters, so a leak before
                ;; a node's last restart is invisible here.
                restarts  (->> (nemesis-windows history)
                               (filter (fn [[f _ _ v]] (and (= :start f) (some? v))))
                               count)]
            (cond-> {:valid?    (empty? undrained)
                     :undrained undrained
                     :stats     stats}
              (pos? restarts)
              (assoc :note (str "covers only the time since each PgDog's last restart ("
                                restarts " start ops)")))))))))
