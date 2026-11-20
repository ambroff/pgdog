(ns pgdog.jepsen.recheck
  "Re-runs Elle on stored append histories with other consistency models,
  without re-running the tests:

    lein run recheck store/pgdog-pooler-append-serializable/2026-... ...
    lein run recheck --consistency-models strict-serializable DIR ...
    lein run recheck --cycle-search-timeout 60000 DIR ...

  With no --consistency-models, each test is checked against the live
  checker's default (append/consistency-models): the strong model for its
  isolation level in pooler mode, the plain one otherwise. Elle's output
  goes to recheck-<models>/ inside each test directory. Prints one line per
  test: valid?, ok count, anomaly types, path."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [elle.list-append :as la]
            [jepsen.history :as h]
            [jepsen.store :as store]
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
            r       (la/check (cond-> {:consistency-models models
                                       :directory          (.getCanonicalPath out)}
                                cycle-search-timeout
                                (assoc :cycle-search-timeout cycle-search-timeout))
                              history)]
        {:dir           dir
         :models        models
         :valid?        (:valid? r)
         :ok            (count (filter #(= :ok (:type %)) history))
         :anomaly-types (:anomaly-types r)}))))

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
