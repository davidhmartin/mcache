(ns mcache.test.cache
  (:use [mcache.cache])
  (:use [mcache.memcached])
  (:use [clojure.test]))

(def mc (net.spy.memcached.MemcachedClient. (list (java.net.InetSocketAddress. "127.0.0.1" 11211))))

(defn clear-cache-fixture [f]
  (.. mc (flush))
  (f)
  (.. mc (flush)))

(use-fixtures :each clear-cache-fixture)

(deftest test-add

  (testing "Add to cache"
    (is (nil? (cache-get mc "key1")))
    ;; cache-add is asynchronous and returns a Future<Boolean>, so we
    ;; use .get to wait for the response
    (is (.get (cache-add mc "key1" "val1")))  
    (is (= "val1" (cache-get mc "key1")))
    (is (not (.get (cache-add mc "key1" "val2"))))
    (is (= "val1" (cache-get mc "key1")))))


(deftest test-add-all
  (testing "Add multiple items to cache"
    (let [input {"one" 1 "two" 2 "three" "the number three"}
          results (cache-add-all mc input)]
      (is (= (set (keys input)) (set (keys results))))
      (doseq [[k v] results]
        (is (.get v))))
    (let [input {"one" "a" "two" "b" "three" 3 "four" 4}
          results (cache-add-all mc input)]
      (is (= (set (keys input)) (set (keys results))))
      (is (false? (.get (get results "one"))))
      (is (false? (.get (get results "two"))))
      (is (false? (.get (get results "three"))))
      (is (true? (.get (get results "four"))))
      (is (= 1 (cache-get mc "one")))
      (is (= 2 (cache-get mc "two")))
      (is (= "the number three" (cache-get mc "three")))
      (is (= 4 (cache-get mc "four"))))))

(deftest test-set
  (testing "Set to cache"
    (is (.get (cache-set mc "setkey1" "val1")))
    (is (= "val1" (cache-get mc "setkey1")))
    (is (.get (cache-set mc "setkey1" "val2")))
    (is (= "val2" (cache-get mc "setkey1")))))

(deftest test-set-all
  (testing "Set multiple to cache"
    (let [input {"one" 1 "two" 2 "three" "the number three"}
          results (cache-set-all mc input)]
      (is (= (set (keys input)) (set (keys results))))
      (doseq [[k v] results]
        (is (.get v))))
    (let [input {"one" "a" "two" "b" "three" 3 "four" 4}
          results (cache-set-all mc input)]
      (is (= (set (keys input)) (set (keys results))))
      (is (true? (.get (get results "one"))))
      (is (true? (.get (get results "two"))))
      (is (true? (.get (get results "three"))))
      (is (true? (.get (get results "four"))))
      (is (= "a" (cache-get mc "one")))
      (is (= "b" (cache-get mc "two")))
      (is (= 3 (cache-get mc "three")))
      (is (= 4 (cache-get mc "four"))))))

(deftest test-replace
  (testing "Replace in cache"
    (is (false? (.get (cache-replace mc "repkey1" "foo"))))
    (is (nil? (cache-get mc "repkey1")))
    (.get (cache-set mc "repkey1" "bar"))
    (is (true? (.get (cache-replace mc "repkey1" "foo"))))
    (is (= "foo" (cache-get mc "repkey1")))))


(deftest test-replace-all
  (testing "Replace multiple"
    (let [input {"one" 1 "two" 2 "three" "the number three"}
          results (cache-set-all mc input)])
    (let [input {"one" "a" "two" "b" "three" 3 "four" 4}
          results (cache-replace-all mc input)]
      (is (= (set (keys input)) (set (keys results))))
      (is (true? (.get (get results "one"))))
      (is (true? (.get (get results "two"))))
      (is (true? (.get (get results "three"))))
      (is (false? (.get (get results "four"))))
      (is (= "a" (cache-get mc "one")))
      (is (= "b" (cache-get mc "two")))
      (is (= 3 (cache-get mc "three")))
      (is (nil? (cache-get mc "four"))))))

(deftest test-delete
  (testing "Delete"
    (cache-set mc "is-there" "thing")
    (is (false? (.get (cache-delete mc "not-there"))))
    (is (true? (.get (cache-delete mc "is-there"))))
    (is (nil? (cache-get mc "is-there")))))

(deftest test-delete-all
  (testing "Delete all"
    (.get (cache-set mc "a" 1))
    (.get (cache-set mc "b" 2))
    (let [resp (cache-delete-all mc ["a" "b" "x"])]
      (is (true? (.get (nth resp 0))))
      (is (true? (.get (nth resp 1))))
      (is (false? (.get (nth resp 2)))))))

(deftest test-incr
  (testing "Increment"
    (is (= -1 (cache-incr mc "n")))
    (.get (cache-set mc "n" 0))
    (is (= 1 (cache-incr mc "n")))
    (is (= 11 (cache-incr mc "n" 10)))
    (is (= 0 (cache-incr mc "m" 1 0)))))

(deftest test-decr
  (testing "Decrement"
    (is (= -1 (cache-decr mc "n")))
    (.get (cache-set mc "n" 0))
    (is (= 0 (cache-decr mc "n")))
    (is (= 10 (cache-decr mc "m" 1 10)))
    (is (= 9 (cache-decr mc "m")))
    (is (= 6 (cache-decr mc "m" 3)))))

(deftest test-get-all
  (testing "Get list of keys"
    (.get (cache-set mc "a" 1))
    (.get (cache-set mc "b" 2))
    (.get (cache-set mc "c" "three"))
    (let [results (cache-get-all mc ["a" "b" "c" "d"])]
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
      (is (= "foo50" (cache-get mc "50")))
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
