(ns mcache.memcached
  "Thin interface to spymemcached"
  (:use mcache.cache)
  (:require [clojure.set]))

(def *EXP* (* 60 60 24 30))

(defn- cache-update-multi
  "Used by add, set, and replace functions which operate on map of key/value pairs.
   Calls the updating function iteratively over each key/val pair, and returns a
   map of key to Future<Boolean>, where the boolean indicates whether the val
   associated with the key was added."
  [cache-updating-fctn mc key-val-map exp]
  (reduce #(assoc %1 (first %2) (second %2)) {}
          (map (fn [k_v] [(first k_v) (cache-updating-fctn mc (first k_v) (second k_v) exp)]) key-val-map)))

(extend-type net.spy.memcached.MemcachedClient
  Memcache

  (default-exp [mc] *EXP*)
  
  (cache-add
    ([mc key value]
       (cache-add mc key value *EXP*))
    ([mc key value exp]
       (.. mc (add key exp value))))
  
  (cache-add-all
    ([mc key-val-map]
       (cache-add-all mc key-val-map *EXP*))
    ([mc key-val-map exp]
       (cache-update-multi cache-add mc key-val-map exp)))
  
  (cache-set
    ([mc key value]
       (.. mc (set key *EXP* value)))
    ([mc key value exp]
       (.. mc (set key exp value))))
  
  (cache-set-all
    ([mc key-val-map] (cache-set-all mc key-val-map *EXP*))
    ([mc key-val-map exp]
       (cache-update-multi cache-set mc key-val-map exp)))
  
  (cache-replace
    ([mc key value] (cache-replace mc key value *EXP*))
    ([mc key value exp]
       (.. mc (replace key exp value))))
  
  (cache-replace-all
    ([mc key-val-map] (cache-replace-all mc key-val-map *EXP*))
    ([mc key-val-map exp]
       (cache-update-multi cache-replace mc key-val-map exp)))

  (cache-delete [mc key]
    (.. mc (delete key)))
  
  (cache-delete-all [mc keys]
    (doall (map #(cache-delete mc %) keys)))

  (cache-incr
    ([mc key]
       (.. mc (incr key 1)))
    ([mc key by]
       (.. mc (incr key by)))
    ([mc key by default]
       (.. mc (incr key by default)))
    ([mc key by default exp]
       (.. mc (incr key by default exp))))
  
  (cache-decr
    ([mc key]
       (.. mc (decr key 1)))
    ([mc key by]
       (.. mc (decr key by)))
    ([mc key by default]
       (.. mc (decr key by default)))
    ([mc key by default exp]
       (.. mc (decr key by default exp))))
  
  (cache-get [mc key]
    (.. mc (get key)))

  (cache-get-all [mc keys]
    (into {} (.. mc (getBulk keys)))) 

)
