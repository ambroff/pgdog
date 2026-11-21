;; JDBC executeBatch with autocommit on, across shards, one statement failing.
;; pgjdbc sends the whole batch as one pipeline with a single Sync, which
;; Postgres runs as one implicit transaction: all or nothing. Used by
;; repro/two_pc.sh batch (PgDog already running on n1, schema from setup).
;;   lein run -m clojure.main repro/batch.clj <k0> <k1>
(require '[next.jdbc :as j])
(let [[k0 k1] (map parse-long *command-line-args*)
      conn (j/get-connection {:dbtype "postgresql" :host "n1" :port 6432 :dbname "jepsen"
                              :user "jepsen" :password "jepsen" :prepareThreshold 0})]
  (.setAutoCommit conn true)
  ;; k1 already exists (inserted by the caller), so the third statement fails.
  (with-open [ps (.prepareStatement conn "INSERT INTO append (k, sk, v) VALUES (?, 0, 'batch')")]
    (doseq [k [k0 (+ k1 1) k1]]
      (.setLong ps 1 k) (.addBatch ps))
    (try (println "executeBatch:" (vec (.executeBatch ps)))
         (catch java.sql.SQLException e
           (println "executeBatch failed:" (.getSQLState e) (.getMessage e)))))
  (.close conn))
