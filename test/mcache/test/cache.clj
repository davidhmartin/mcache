(ns mcache.test.cache
  (:use [mcache.cache])
  (:use [clojure.test])
  (:import [clojure.core.cache CacheProtocol]
           [mcache.cache MemcachedCache])
)

(def spy (net.spy.memcached.MemcachedClient. (list (java.net.InetSocketAddress. "127.0.0.1" 11211))))
(def mc (make-memcached-cache spy))

(defn clear-cache-fixture
  "Flushes the cache before and after. NOTE: Flush is asynchronous, so
  it can't really be relied on to bring the cache to a clean state
  between tests. An acceptable workaround is to avoid using the same keys in
  different tests."
  [f]
  (clear mc)
  (f)
  (clear mc))

(use-fixtures :each clear-cache-fixture)


(defn do-test-lookup [mc]
  (.get (put mc "a" 1))
  (.get (put mc "b" 2))
  (is (= 1 (.lookup mc "a"))))


(defn do-test-put-if-absent [mc]
    (is (nil? (fetch mc "key1")))
    ;; put-if-add is asynchronous and returns a Future<Boolean>, so we
    ;; use .get to wait for the response
    (is (.get (put-if-absent mc "key1" "val1")))  
    (is (= "val1" (fetch mc "key1")))
    (is (not (.get (put-if-absent mc "key1" "val2"))))
    (is (= "val1" (fetch mc "key1"))))


(defn do-test-put-all-if-absent [mc]
    (let [input {"one" 1 "two" 2 "three" "the number three"}
          results (put-all-if-absent mc input)]
      (is (= (set (keys input)) (set (keys results))))
      (doseq [[k v] results]
        (is (.get v))))
    (let [input {"one" "a" "two" "b" "three" 3 "four" 4}
          results (put-all-if-absent mc input)]
      (is (= (set (keys input)) (set (keys results))))
      (is (false? (.get (get results "one"))))
      (is (false? (.get (get results "two"))))
      (is (false? (.get (get results "three"))))
      (is (true? (.get (get results "four"))))
      (is (= 1 (fetch mc "one")))
      (is (= 2 (fetch mc "two")))
      (is (= "the number three" (fetch mc "three")))
      (is (= 4 (fetch mc "four")))))

(defn do-test-put [mc]
    (is (.get (put mc "setkey1" "val1")))
    (is (= "val1" (fetch mc "setkey1")))
    (is (.get (put mc "setkey1" "val2")))
    (is (= "val2" (fetch mc "setkey1"))))

(defn do-test-put-all [mc]
    (let [input {"one" 1 "two" 2 "three" "the number three"}
          results (put-all mc input)]
      (is (= (set (keys input)) (set (keys results))))
      (doseq [[k v] results]
        (is (.get v))))
    (let [input {"one" "a" "two" "b" "three" 3 "four" 4}
          results (put-all mc input)]
      (is (= (set (keys input)) (set (keys results))))
      (is (true? (.get (get results "one"))))
      (is (true? (.get (get results "two"))))
      (is (true? (.get (get results "three"))))
      (is (true? (.get (get results "four"))))
      (is (= "a" (fetch mc "one")))
      (is (= "b" (fetch mc "two")))
      (is (= 3 (fetch mc "three")))
      (is (= 4 (fetch mc "four")))))

(defn do-test-put-if-present [mc]
    (is (false? (.get (put-if-present mc "repkey1" "foo"))))
    (is (nil? (fetch mc "repkey1")))
    (.get (put mc "repkey1" "bar"))
    (is (true? (.get (put-if-present mc "repkey1" "foo"))))
    (is (= "foo" (fetch mc "repkey1"))))


(defn do-test-put-all-if-present [mc]
    (let [input {"one" 1 "two" 2 "three" "the number three"}
          results (put-all mc input)])
    (let [input {"one" "a" "two" "b" "three" 3 "four" 4}
          results (put-all-if-present mc input)]
      (is (= (set (keys input)) (set (keys results))))
      (is (true? (.get (get results "one"))))
      (is (true? (.get (get results "two"))))
      (is (true? (.get (get results "three"))))
      (is (false? (.get (get results "four"))))
      (is (= "a" (fetch mc "one")))
      (is (= "b" (fetch mc "two")))
      (is (= 3 (fetch mc "three")))
      (is (nil? (fetch mc "four")))))

(defn do-test-delete [mc]
    (put mc "is-there" "thing")
    (is (false? (.get (delete mc "not-there"))))
    (is (true? (.get (delete mc "is-there"))))
    (is (nil? (fetch mc "is-there"))))

(defn do-test-delete-all [mc]
    (.get (put mc "a" 1))
    (.get (put mc "b" 2))
    (let [resp (delete-all mc ["a" "b" "x"])]
      (is (true? (.get (nth resp 0))))
      (is (true? (.get (nth resp 1))))
      (is (false? (.get (nth resp 2))))))

(defn do-test-incr [mc]
    (is (= -1 (incr mc "n")))
    (.get (put mc "n" "0"))
    (is (= 1 (incr mc "n")))
    (is (= 11 (incr mc "n" 10)))
    (is (= 0 (incr mc "m" 1 0))))

(defn do-test-decr [mc]
    (is (= -1 (decr mc "nn")))
    (.get (put mc "nn" 0))
    (is (= 0 (decr mc "nn")))
    (is (= 10 (decr mc "mm" 1 10)))
    (is (= 9 (decr mc "mm")))
    (is (= 6 (decr mc "mm" 3))))

(defn do-test-get-all [mc]
    (.get (put mc "a" 1))
    (.get (put mc "b" 2))
    (.get (put mc "c" "three"))
    (let [results (fetch-all mc ["a" "b" "c" "d"])]
      (is (= 1 (get results "a")))
      (is (= 2 (get results "b")))
      (is (= "three" (get results "c")))
      (is (nil? (get results "d")))))

(defn do-test-with-cache [mc]
    (letfn [(qfcn [id]
              (if (< (Integer/parseInt id) 100)
                (str "foo" id)
                nil))]
      (is (nil? (with-cache mc "200" qfcn)))
      (is (= "foo50" (with-cache mc "50" qfcn)))
      (is (= "foo50" (fetch mc "50")))
      (is (= "foo50" (with-cache mc "50" qfcn)))))


(defn do-test-cache-fetch [mc]
    (letfn [(qfcn [id]
              (if (< id 100)
                (str "foo" id)
                nil))]
      (is (nil? (cache-fetch mc 200 qfcn str)))
      (is (= "foo50" (cache-fetch mc 50 qfcn str)))
      (is (= "foo50" (fetch mc "50")))
      (is (= "foo50" (cache-fetch mc 50 qfcn str)))))

(defn do-test-cache-fetch-all [mc]
    (letfn [(qfcn [ids]
              (zipmap ids (map 
                      #(if (< % 100)
                        (str "foo" %)
                        nil) ids)))]
      (is (= {1 "foo1" 4 "foo4" 5 "foo5" 6 "foo6"}
             (cache-fetch-all mc [1 4 120 5 300 6] qfcn str)))
      (is (= {1 "foo1" 5 "foo5" 19 "foo19"}
             (cache-fetch-all mc [1 120 5 19] qfcn str)))))


;;;;;;;;;;;;;;;;;;;

;; Unit tests for MemcachedClient extending the Memcache protocol

(deftest test-put-if-absent
  (testing "put-if-absent"
    (do-test-put-if-absent spy)))

(deftest test-put-all-if-absent
  (testing "Add multiple items to cache"
    (do-test-put-all-if-absent spy)))

(deftest test-put
  (testing "Put into cache"
    (do-test-put spy)))

(deftest test-put-all
  (testing "Put multiple into cache"
    (do-test-put-all spy)))

(deftest test-put-if-present
  (testing "Replace in cache" 
    (do-test-put-if-present spy)))

(deftest test-put-all-if-present  ;;;;;
  (testing "Replace multiple"
    (do-test-put-all-if-present spy)))

(deftest test-delete
  (testing "Delete"
    (do-test-delete spy)))

(deftest test-delete-all
  (testing "Delete all"
    (do-test-delete-all spy)))

(deftest test-incr
  (testing "Increment"
    (do-test-incr spy)))

(deftest test-decr
  (testing "Decrement"
    (do-test-decr spy)))

(deftest test-get-all
  (testing "Get list of keys"
    (do-test-get-all spy)))

(deftest test-with-cache
  (testing "with-cache macro"
    (do-test-with-cache spy)))

(deftest test-cache-fetch
  (testing "Cached fetch"
    (do-test-cache-fetch spy)))

(deftest test-cache-fetch-all
  (testing "Cached fetch"
    (do-test-cache-fetch-all spy)))


;; now test MemcachedCache, which integrates the same functionality
;; into clojure.core.cache 

(deftest test-lookup-cache
  (testing "lookup"
    (do-test-lookup mc)))

(deftest test-put-if-absent-cache
  (testing "put-if-absent"
    (do-test-put-if-absent mc)))

(deftest test-put-all-if-absent-cache
  (testing "Add multiple items to cache"
    (do-test-put-all-if-absent mc)))

(deftest test-put-cache
  (testing "Put into cache"
    (do-test-put mc)))

(deftest test-put-all-cache
  (testing "Put multiple into cache"
    (do-test-put-all mc)))

(deftest test-put-if-present-cache
  (testing "Replace in cache" 
    (do-test-put-if-present mc)))

(deftest test-put-all-if-present  ;;;;;
  (testing "Replace multiple"
    (do-test-put-all-if-present mc)))

(deftest test-delete-cache
  (testing "Delete"
    (do-test-delete mc)))

(deftest test-delete-all-cache
  (testing "Delete all"
    (do-test-delete-all mc)))

(deftest test-incr-cache
  (testing "Increment"
    (do-test-incr mc)))

(deftest test-decr-cache
  (testing "Decrement"
    (do-test-decr mc)))

(deftest test-get-all-cache
  (testing "Get list of keys"
    (do-test-get-all mc)))

(deftest test-with-cache-cache
  (testing "with-cache macro"
    (do-test-with-cache mc)))

(deftest test-cache-fetch-cache
  (testing "Cached fetch"
    (do-test-cache-fetch mc)))

(deftest test-cache-fetch-all-cache
  (testing "Cached fetch"
    (do-test-cache-fetch-all mc)))


