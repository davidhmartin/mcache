(defproject mcache "0.1.0"
  :description "A protocol-based interface to memory cache clients."
  :dependencies [[org.clojure/clojure "1.2.0"]
                 [spy/spymemcached "2.7.3"]]
  :repositories {"memcached" "http://files.couchbase.com/maven2/"}
  :dev-dependencies [[org.clojars.gjahad/debug-repl "0.3.1"]])