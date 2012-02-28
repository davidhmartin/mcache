(defproject mcache "0.2.0"
  :description "A protocol-based interface to memory cache clients."
  :url "https://github.com/davidhmartin/mcache"
  :dependencies [[org.clojure/clojure "1.2.1"]
                 [org.clojure/core.cache "0.5.0"]
                 [spy/spymemcached "2.7.3"]]
  :repositories {"memcached" "http://files.couchbase.com/maven2/"})