(ns mcache.test.cache
  (:use [mcache.cache])
  (:use [clojure.test])
  ;; (:require [clojure.core.cache.tests :as coretests])
  (:import [clojure.core.cache CacheProtocol]
           [mcache.cache MemcachedCache])
)

(def mc (net.spy.memcached.MemcachedClient. (list (java.net.InetSocketAddress. "127.0.0.1" 11211))))
(def mcache (make-memcached-cache mc))

(defn clear-cache-fixture
  "Flushes the cache before and after. NOTE: Flush is asynchronous, so
  it can't really be relied on to bring the cache to a clean state
  between tests. Therefore, it is best to avoid using the same keys in
  different tests."
  [f]
  (.. mc (flush))
  (f)
  (.. mc (flush)))

(use-fixtures :each clear-cache-fixture)

(deftest test-put-if-absent

  (testing "put-if-absent"
    (is (nil? (fetch mc "key1")))
    ;; put-if-add is asynchronous and returns a Future<Boolean>, so we
    ;; use .get to wait for the response
    (is (.get (put-if-absent mc "key1" "val1")))  
    (is (= "val1" (fetch mc "key1")))
    (is (not (.get (put-if-absent mc "key1" "val2"))))
    (is (= "val1" (fetch mc "key1")))))


(deftest test-put-all-if-absent
  (testing "Add multiple items to cache"
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
      (is (= 4 (fetch mc "four"))))))

(deftest test-put
  (testing "Put into cache"
    (is (.get (put mc "setkey1" "val1")))
    (is (= "val1" (fetch mc "setkey1")))
    (is (.get (put mc "setkey1" "val2")))
    (is (= "val2" (fetch mc "setkey1")))))

(deftest test-put-all
  (testing "Put multiple into cache"
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
      (is (= 4 (fetch mc "four"))))))

(deftest test-put-if-present
  (testing "Replace in cache"
    (is (false? (.get (put-if-present mc "repkey1" "foo"))))
    (is (nil? (fetch mc "repkey1")))
    (.get (put mc "repkey1" "bar"))
    (is (true? (.get (put-if-present mc "repkey1" "foo"))))
    (is (= "foo" (fetch mc "repkey1")))))


(deftest test-put-all-if-present  ;;;;;
  (testing "Replace multiple"
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
      (is (nil? (fetch mc "four"))))))

(deftest test-delete
  (testing "Delete"
    (put mc "is-there" "thing")
    (is (false? (.get (delete mc "not-there"))))
    (is (true? (.get (delete mc "is-there"))))
    (is (nil? (fetch mc "is-there")))))

(deftest test-delete-all
  (testing "Delete all"
    (.get (put mc "a" 1))
    (.get (put mc "b" 2))
    (let [resp (delete-all mc ["a" "b" "x"])]
      (is (true? (.get (nth resp 0))))
      (is (true? (.get (nth resp 1))))
      (is (false? (.get (nth resp 2)))))))

(deftest test-incr
  (testing "Increment"
    (is (= -1 (incr mc "n")))
    (.get (put mc "n" 0))
    (is (= 1 (incr mc "n")))
    (is (= 11 (incr mc "n" 10)))
    (is (= 0 (incr mc "m" 1 0)))))

(deftest test-decr
  (testing "Decrement"
    (is (= -1 (decr mc "nn")))
    (.get (put mc "nn" 0))
    (is (= 0 (decr mc "nn")))
    (is (= 10 (decr mc "mm" 1 10)))
    (is (= 9 (decr mc "mm")))
    (is (= 6 (decr mc "mm" 3)))))

(deftest test-get-all
  (testing "Get list of keys"
    (.get (put mc "a" 1))
    (.get (put mc "b" 2))
    (.get (put mc "c" "three"))
    (let [results (fetch-all mc ["a" "b" "c" "d"])]
      (is (= 1 (get results "a")))
      (is (= 2 (get results "b")))
      (is (= "three" (get results "c")))
      (is (nil? (get results "d"))))))


(deftest test-cache-fetch
  (testing "Cached fetch"
    (letfn [(qfcn [id]
              (if (< id 100)
                (str "foo" id)
                nil))]
      (is (nil? (cache-fetch mc 200 qfcn str)))
      (is (= "foo50" (cache-fetch mc 50 qfcn str)))
      (is (= "foo50" (fetch mc "50")))
      (is (= "foo50" (cache-fetch mc 50 qfcn str))))))

(deftest test-cache-fetch-all
  (testing "Cached fetch"
    (letfn [(qfcn [ids]
              (zipmap ids (map 
                      #(if (< % 100)
                        (str "foo" %)
                        nil) ids)))]
      (is (= {1 "foo1" 4 "foo4" 5 "foo5" 6 "foo6"}
             (cache-fetch-all mc [1 4 120 5 300 6] qfcn str)))
      (is (= {1 "foo1" 5 "foo5" 19 "foo19"}
             (cache-fetch-all mc [1 120 5 19] qfcn str))))))

(deftest test-lookup
  (testing "lookup"
    (.get (put mc "a" 1))
    (.get (put mc "b" 2))
    (is (= 1 (CacheProtocol/.lookup mcache "a")))))




;; (deftest test-memcached-cache-ilookup
;;   (testing "that the MemcachedCache can lookup via keywords"
;;     (coretests/do-ilookup-tests (seed (MemcachedCache. mc) coretests/small-map)))
;;   (testing "that the MemcachedCache can .lookup"
;;     (coretests/do-dot-lookup-tests (seed (MemcachedCache. mc) coretests/small-map)))
;;   (testing "assoc and dissoc for MemcachedCache"
;;     (coretests/do-assoc (MemcachedCache. mc))
;;     (coretests/do-dissoc (seed (MemcachedCache. mc) {:a 1 :b 2})))
;;   (testing "that get and cascading gets work for MemcachedCache"
;;     (coretests/do-getting (seed (MemcachedCache. mc) coretests/big-map)))
;;   (testing "that finding works for MemcachedCache"
;;     (coretests/do-finding (seed (MemcachedCache. mc) coretests/small-map)))
;;   (testing "that contains? works for BasicCache"
;;     (coretests/do-contains (BasicCache. coretests/small-map))))


;; (.valAt mcache "x" (assoc mcache "x" 13))