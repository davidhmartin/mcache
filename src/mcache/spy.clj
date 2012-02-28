(ns mcache.spy
  "Memcache protocol implementation extending net.spy.memcached.MemcachedClient"
  (:use [mcache.core]
        [mcache.util :only (cache-update-multi)]))

(extend-type net.spy.memcached.MemcachedClient
  Memcache
  (default-exp [mc] DEFAULT-EXP)
  
  (put-if-absent
    ([mc key value]
       (put-if-absent mc key value DEFAULT-EXP))
    ([mc key value exp]
       (.. mc (add key exp value))))
  
  (put-all-if-absent
    ([mc key-val-map]
       (put-all-if-absent mc key-val-map DEFAULT-EXP))
    ([mc key-val-map exp]
       (cache-update-multi put-if-absent mc key-val-map exp)))
  
  (put
    ([mc key value]
       (.. mc (set key DEFAULT-EXP value)))
    ([mc key value exp]
       (.. mc (set key exp value))))
  
  (put-all
    ([mc key-val-map] (put-all mc key-val-map DEFAULT-EXP))
    ([mc key-val-map exp]
       (cache-update-multi put mc key-val-map exp)))
  
  (put-if-present
    ([mc key value] (put-if-present mc key value DEFAULT-EXP))
    ([mc key value exp]
       (.. mc (replace key exp value))))
  
  (put-all-if-present
    ([mc key-val-map] (put-all-if-present mc key-val-map DEFAULT-EXP))
    ([mc key-val-map exp]
       (cache-update-multi put-if-present mc key-val-map exp)))

  (delete [mc key]
    (.. mc (delete key)))
  
  (delete-all [mc keys]
    (doall (map #(delete mc %) keys)))

  (fetch [mc key]
    (.. mc (get key)))

  (fetch-all [mc keys]
    (into {} (.. mc (getBulk keys)))) 

  (clear [mc] (.. mc (flush)))

  )







