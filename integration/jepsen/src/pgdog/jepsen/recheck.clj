(ns pgdog.jepsen.recheck
  "Re-runs Elle on stored append histories with other consistency models,
  without re-running the tests:

    lein run recheck store/pgdog-pooler-append-serializable/2026-... ...
    lein run recheck --consistency-models strict-serializable DIR ...
    lein run recheck --cycle-search-timeout 60000 DIR ...

  With no --consistency-models, each test is checked against the live
  checker's default (append/consistency-models): the strong model for its
  isolation level. Sharded tests are checked per shard (shard-<s>/). Elle's output
  goes to recheck-<models>/ inside each test directory. Prints one line per
  test: valid?, ok count, anomaly types, path."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [elle.list-append :as la]
            [jepsen.history :as h]
            [jepsen.store :as store]
            [jepsen.checker :as checker]
            [pgdog.jepsen.client :as pc]
            [pgdog.jepsen.workload.append :as append]))

(defn recheck
  [{:keys [consistency-models cycle-search-timeout]} dir]
  (let [test (store/test (str dir))]
    (if-not (= :append (:workload test))
      {:dir dir, :skipped (str "workload " (:workload test))}
      (let [models  (or (seq consistency-models)
                        (append/consistency-models (select-keys test [:mode :isolation])))
            out     (io/file dir (str "recheck-" (str/join "+" (map name models))))
            history (h/filter-f :txn (:history test))
            ;; Sharded tests are checked per shard, like the live checker.
            parts   (if (= :sharded (:mode test))
                      (into (sorted-map)
                            (map (fn [s] [s (h/filter #(= s (:shard %)) history)]))
                            (sort (distinct (keep :shard history))))
                      {nil history})
            rs      (for [[s part] parts]
                      (la/check (cond-> {:consistency-models models
                                         :directory          (.getCanonicalPath
                                                               (if s (io/file out (str "shard-" s)) out))}
                                  cycle-search-timeout
                                  (assoc :cycle-search-timeout cycle-search-timeout))
                                part))
            valid   (map :valid? rs)]
        {:dir           dir
         :models        models
         :valid?        (cond (every? true? valid) true
                              (some false? valid)  false
                              :else                :unknown)
         :ok            (count (filter #(= :ok (:type %)) history))
         :anomaly-types (vec (distinct (mapcat :anomaly-types rs)))}))))

(defn run-cmd!
  [{:keys [options arguments]}]
  (doseq [dir arguments]
    (let [r (try (recheck options dir)
                 (catch Exception e {:dir dir, :error (.getMessage e)}))]
      (println (str/join "\t" [(cond (:skipped r) "skipped"
                                     (:error r)   "error"
                                     :else        (:valid? r))
                               (or (:ok r) "-")
                               (pr-str (or (:anomaly-types r) (:skipped r) (:error r)))
                               (pr-str (:models r))
                               dir]))
      (flush))))

(defn reanalyze
  "Re-runs the workload's non-Elle append checkers (backends, isolation, lost,
  placement) on a stored test, with the current checker code. For fixing a
  checker without re-running the test."
  [dir]
  (let [test    (store/test (str dir))
        history (:history test)
        cs      (cond-> {:backends  (pc/backend-checker)
                         :isolation (pc/isolation-checker)
                         :lost      (append/lost-appends-checker)}
                  (= :sharded (:mode test)) (assoc :placement (pc/placement-checker)))]
    (into (sorted-map)
          (map (fn [[k c]]
                 (let [r (checker/check c test history {})]
                   [k (select-keys r [:valid? :attribution :unexplained-count :final-reads
                                      :split-count :wrong-count :lost-count :misrouted-count
                                      :misplaced-count :mismatch-count :xacts-per-txn])])))
          cs)))

(defn reanalyze-cmd!
  [{:keys [arguments]}]
  (doseq [dir arguments]
    (let [r (try (reanalyze dir) (catch Exception e {:error (.getMessage e)}))]
      (println (str/join "\t" [(if (:error r) "error" (every? true? (map :valid? (vals r))))
                                (pr-str r) dir]))
      (flush))))
