(ns pgdog.jepsen.client
  "JDBC plumbing shared by every workload: connections through PgDog (or
  straight to Postgres with --control), transactions, and the error
  classification that decides whether a failed op is :fail (definitely did not
  happen) or :info (may or may not have happened)."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen.checker :as checker]
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

(defn run-txn!
  "Runs (f conn) inside a transaction at the test's isolation level, and
  completes `op`. Returns the completed op. Reconnects lazily: on a connection
  error `conn-atom` is reset to nil and the next call opens a new one.

  --isolation-mode :txn issues SET TRANSACTION ISOLATION LEVEL as the first
  statement of every transaction. :session uses JDBC's setTransactionIsolation,
  which pgjdbc sends as SET SESSION CHARACTERISTICS: through a transaction
  pooler, that only works if PgDog carries session state across server
  connections (claim C2)."
  [test node conn-atom op f]
  (let [phase (volatile! :connect)]
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
        (let [seen  (:iso (j/execute-one!
                            conn ["SELECT current_setting('transaction_isolation') AS iso"]))
              value (f conn)]
          (vreset! phase :commit)
          (.commit conn)
          (assoc op :type :ok, :value value, :isolation-seen seen)))
      (catch Throwable e
        (let [conn @conn-atom]
          (if (or (nil? conn) (connection-error? e))
            (do (close! conn) (reset! conn-atom nil))
            (try (.rollback ^Connection conn)
                 (catch Throwable _
                   (close! conn) (reset! conn-atom nil)))))
        (cond
          (= :connect @phase)
          (assoc op :type :fail, :error [:connect (error-type e) (.getMessage e)])

          (definite-failure? test @phase e)
          (assoc op :type :fail, :error [@phase (error-type e) (.getMessage e)])

          :else
          (assoc op :type :info, :error [@phase (error-type e) (.getMessage e)]))))))

(defn isolation-checker
  "Checks that every ok transaction ran at the isolation level the test asked
  for, as reported by the server inside the transaction (:isolation-seen)."
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [want  (str/lower-case (isolation-sql (:isolation test)))
            oks   (filter #(and (= :ok (:type %)) (contains? % :isolation-seen)) history)
            wrong (remove #(= want (:isolation-seen %)) oks)]
        {:valid?      (empty? wrong)
         :expected    want
         :ok-count    (count oks)
         :seen        (frequencies (map :isolation-seen oks))
         :wrong-count (count wrong)
         :examples    (vec (take 10 wrong))}))))
