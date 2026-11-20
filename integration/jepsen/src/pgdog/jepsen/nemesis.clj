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
            [pgdog.jepsen.client :as pc]
            [pgdog.jepsen.db :as db]))

(defn client-targets
  "The nodes clients connect to, and the port: PgDog nodes, or the Postgres
  node with --control."
  [test]
  (if (:control test)
    [(vec (take 1 (role/nodes test :postgres))) db/pg-port]
    [(role/nodes test :pgdog) db/pgdog-port]))

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
