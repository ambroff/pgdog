(ns pgdog.jepsen.workload.session-leak
  "Does session state follow the client across pooled server connections, and
  only that client? (Claim C2, hypothesis H9 in PLAN.org.)

  Each client holds a desired session state: a default isolation level plus a
  lock_timeout value unique to that :set op, applied with one of several
  mechanisms (see `mechanisms`). Ops:

    {:f :set}    pick a new mechanism, level and lock_timeout, and apply them
    {:f :check}  read transaction_isolation and lock_timeout, either inside an
                 explicit transaction or in autocommit

  A :check completes with {:expected .., :observed ..}. The checker flags
  every ok :check where they differ. Since lock_timeout values are unique, an
  observed value that some other client set is attributed to that client
  (a leak), as opposed to a setting that was simply lost."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]]
            [jepsen.random :as rand]
            [next.jdbc :as j]
            [pgdog.jepsen.client :as pc])
  (:import (java.sql Connection)))

(def mechanisms
  "How a client applies its session state.

  :reset            RESET both settings (expects the server defaults)
  :set              SET default_transaction_isolation / SET lock_timeout
  :set-config       SELECT set_config('..', '<literal>', false)
  :set-config-bind  SELECT set_config(?, ?, false) with bind parameters
  :characteristics  SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL
                    (lock_timeout via plain SET)
  :jdbc             Connection.setTransactionIsolation, which pgjdbc sends as
                    SET SESSION CHARACTERISTICS (lock_timeout via plain SET)
  :startup          startup packet `options` (-c name=value); applied by
                    opening a new connection"
  [:reset :set :set-config :set-config-bind :characteristics :jdbc :startup])

(def default-state
  {:isolation "read committed", :lock-timeout "0"})

(def levels ["read committed" "repeatable read" "serializable"])

(def lock-timeout-counter
  "Source of unique lock_timeout values across all clients. Values end in 7 so
  Postgres never renders them in seconds (e.g. 2000ms -> 2s)."
  (atom 0))

(defn next-lock-timeout []
  (str (+ 7 (* 10 (swap! lock-timeout-counter inc))) "ms"))

(defn random-desired
  "A new desired state, using one of the test's --session-mechanisms."
  [test]
  (let [mechanism (rand/nth (:session-mechanisms test))]
    (if (= :reset mechanism)
      (assoc default-state :mechanism :reset)
      {:mechanism    mechanism
       :isolation    (rand/nth levels)
       :lock-timeout (next-lock-timeout)})))

(defn expected [desired]
  (select-keys desired [:isolation :lock-timeout]))

(def isolation-jdbc
  {"read committed"  Connection/TRANSACTION_READ_COMMITTED
   "repeatable read" Connection/TRANSACTION_REPEATABLE_READ
   "serializable"    Connection/TRANSACTION_SERIALIZABLE})

(defn startup-options
  "Startup `options` string. Postgres splits it on whitespace, so spaces in
  values are backslash-escaped."
  [{:keys [isolation lock-timeout]}]
  (str "-c default_transaction_isolation=" (str/replace isolation " " "\\ ")
       " -c lock_timeout=" lock-timeout))

(defn apply-state!
  "Applies `desired` to an open, autocommit connection. :startup is applied
  when the connection is opened, so it's a no-op here."
  [^Connection conn {:keys [mechanism isolation lock-timeout]}]
  (let [exec! (fn [& sql] (j/execute! conn (vec sql)))]
    (case mechanism
      :startup nil

      :reset
      (do (exec! "RESET default_transaction_isolation")
          (exec! "RESET lock_timeout"))

      :set
      (do (exec! (str "SET default_transaction_isolation = '" isolation "'"))
          (exec! (str "SET lock_timeout = '" lock-timeout "'")))

      :set-config
      (do (exec! (str "SELECT set_config('default_transaction_isolation', '"
                      isolation "', false)"))
          (exec! (str "SELECT set_config('lock_timeout', '" lock-timeout "', false)")))

      :set-config-bind
      (do (exec! "SELECT set_config(?, ?, false)" "default_transaction_isolation" isolation)
          (exec! "SELECT set_config(?, ?, false)" "lock_timeout" lock-timeout))

      :characteristics
      (do (exec! (str "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL "
                      (str/upper-case isolation)))
          (exec! (str "SET lock_timeout = '" lock-timeout "'")))

      :jdbc
      (do (.setTransactionIsolation conn (isolation-jdbc isolation))
          (exec! (str "SET lock_timeout = '" lock-timeout "'"))))))

(defn connect!
  "Opens a connection that carries `desired`: startup options for :startup,
  otherwise a plain connection with the state applied by SQL."
  [test node desired]
  (let [startup? (= :startup (:mechanism desired))
        conn     (pc/open test node (if startup?
                                      {:options (startup-options desired)}
                                      {}))]
    (try
      (apply-state! conn desired)
      {:conn conn, :startup? startup?}
      (catch Throwable e
        (pc/close! conn)
        (throw e)))))

(defn observe
  "Reads the settings the server is actually using. In :txn mode the read
  runs inside an explicit transaction."
  [^Connection conn mode]
  (let [q   (str "SELECT current_setting('transaction_isolation') AS iso, "
                 "current_setting('lock_timeout') AS lt, "
                 "pg_backend_pid() AS pid")
        row (if (= :txn mode)
              (try
                (.setAutoCommit conn false)
                (let [row (j/execute-one! conn [q])]
                  (.commit conn)
                  row)
                (finally
                  (.setAutoCommit conn true)))
              (j/execute-one! conn [q]))]
    {:observed {:isolation    (:iso row)
                :lock-timeout (:lt row)}
     :pid      (:pid row)}))

(defn fail-op
  "Completes `op` after an exception, using the shared classification."
  [test phase op e]
  (assoc op
         :type  (if (or (= :connect phase) (pc/definite-failure? test phase e))
                  :fail :info)
         :error [phase (pc/error-type e) (.getMessage ^Throwable e)]))

(defrecord Client [node state]
  ;; state: atom of {:desired .., :conn .., :startup? ..}
  client/Client
  (open! [_ test node]
    (Client. node (atom {:desired (random-desired test)})))

  (setup! [_ test])

  (invoke! [_ test op]
    (let [phase (volatile! :connect)]
      (try
        (case (:f op)
          :set
          (let [desired (random-desired test)
                {:keys [conn startup?]} @state]
            (swap! state assoc :desired desired)
            ;; Startup state can only be changed by reconnecting; RESET on a
            ;; startup connection would restore the startup values, not the
            ;; server defaults.
            (if (or (nil? conn) startup? (= :startup (:mechanism desired)))
              (do (pc/close! conn)
                  (swap! state dissoc :conn :startup?)
                  (swap! state merge (connect! test node desired)))
              (do (vreset! phase :exec)
                  (apply-state! conn desired)))
            (assoc op :type :ok, :value desired))

          :check
          (let [{:keys [desired conn]} @state
                conn (or conn
                         (let [c (connect! test node desired)]
                           (swap! state merge c)
                           (:conn c)))
                mode (rand/nth [:txn :auto])]
            (vreset! phase :exec)
            (assoc op :type :ok
                   :value (assoc (observe conn mode)
                                 :mode      mode
                                 :mechanism (:mechanism desired)
                                 :expected  (expected desired)))))
        (catch Throwable e
          ;; Whatever happened, the session's state is now unknown: drop the
          ;; connection and re-apply the desired state on the next op.
          ;; A failed :set still records the state it chose, since that state
          ;; is applied on reconnect.
          (pc/close! (:conn @state))
          (swap! state dissoc :conn :startup?)
          (cond-> (fail-op test @phase op e)
            (= :set (:f op)) (assoc :value (:desired @state)))))))

  (teardown! [_ test])

  (close! [_ test]
    (pc/close! (:conn @state))))

;; ---------------------------------------------------------------------------
;; Checker

(defn setters
  "Map of lock_timeout value -> process that set it, from :set completions
  and from what :check ops expected (which covers each client's initial
  state, chosen in open! without a :set op). The default (set by :reset)
  belongs to nobody."
  [history]
  (->> history
       (keep (fn [{:keys [f value process]}]
               (let [lt (case f
                          :set   (when (map? value) (:lock-timeout value))
                          :check (get-in value [:expected :lock-timeout])
                          nil)]
                 (when (and lt (not= lt (:lock-timeout default-state)))
                   [lt process]))))
       (into {})))

(defn mismatches
  "Every [param op] where an ok :check observed something other than what the
  client asked for."
  [history]
  (for [op    history
        :when (and (= :ok (:type op)) (= :check (:f op)))
        param [:isolation :lock-timeout]
        :let  [{:keys [expected observed]} (:value op)]
        :when (not= (get expected param) (get observed param))]
    [param op]))

(defn classify
  "Describes one mismatch: whose value (if anyone's) the client saw instead."
  [setters [param op]]
  (let [{:keys [expected observed mechanism mode pid]} (:value op)
        seen     (get observed param)
        owner    (when (= :lock-timeout param) (get setters seen))]
    {:param     param
     :mechanism mechanism
     :mode      mode
     :kind      (cond
                  (= seen (get default-state param)) :reverted-to-default
                  (= owner (:process op))            :own-stale-value
                  owner                              :leaked-from-other-client
                  :else                              :wrong-value)
     :expected  (get expected param)
     :observed  seen
     :from      owner
     :process   (:process op)
     :index     (:index op)
     :time      (:time op)
     :pid       pid}))

(defn checker
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [setters (setters history)
            found   (map (partial classify setters) (mismatches history))
            checks  (->> history
                         (filter #(and (= :ok (:type %)) (= :check (:f %)))))]
        {:valid?          (empty? found)
         :check-count     (count checks)
         :checks-by-mechanism (frequencies (map (comp :mechanism :value) checks))
         :mismatch-count  (count found)
         :by-param+mechanism (->> found
                                  (group-by (juxt :param :mechanism))
                                  (map (fn [[k v]] [k (frequencies (map :kind v))]))
                                  (into (sorted-map)))
         :by-kind         (frequencies (map :kind found))
         :examples        (->> found
                               (group-by (juxt :param :mechanism :kind))
                               vals
                               (map first)
                               (sort-by :index)
                               (take 20)
                               vec)}))))

(defn workload
  [opts]
  {:client    (Client. nil nil)
   :generator (fn [] (if (< (rand/double) 0.2) {:f :set} {:f :check}))
   :checker   (checker)})
