(defproject pgdog-jepsen "0.1.0-SNAPSHOT"
  :description "Jepsen tests for PgDog's consistency claims"
  :license {:name "AGPL-3.0-only"}
  :main pgdog.jepsen.core
  :jvm-opts ["-Xmx8g" "-Djava.awt.headless=true" "-server"]
  :dependencies [[org.clojure/clojure "1.12.0"]
                 [jepsen "0.3.15"]
                 [com.github.seancorfield/next.jdbc "1.4.72"]
                 [org.postgresql/postgresql "42.7.7"]]
  :repl-options {:init-ns pgdog.jepsen.core})
