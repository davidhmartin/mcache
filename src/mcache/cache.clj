(ns mcache.cache
  "Protocol for memory cache, and spymemcached implementation"
  (:use [clojure.core.cache :only (defcache)])
  (:require [clojure.set]
            [clojure.core.cache])
  (:import [clojure.core.cache CacheProtocol])
  )

(def DEFAULT-EXP (* 60 60 24 30))


(defprotocol Memcache
  "This is a protocol for memcached clients

   The functions which insert values into the cache may optionally
   accept an expiration, the semantics of which are assumed to 
   follow the memcached protocol, wherein the time unit is seconds,
   and a value between 1 and 30 days is treated as time-to-live,
   and any higher value is treated as a unix-style timestamp (seconds
   since Jan 1 1970)."

  (default-exp [mc]
    "Return the default expiration")
  
  (put-if-absent 
    [mc key value]
    [mc key value exp]
    "Add a key/value to the cache, if the key is not already there.")
  
  (put-all-if-absent
    [mc key-val-map]
    [mc key-val-map exp]
    "Add multiple keys/values to the cache, if not already there.")
  
  (put
    [mc key value]
    [mc key value exp]
    "Set a key/value. If a value is already cached with that key, it is replaced.")
  
  (put-all
    [mc key-val-map]
    [mc key-val-map exp]
    "Sets multiple keys/values, overwriting existing values if any.")
  
  (put-if-present
    [mc key value]
    [mc key value exp]
    "Replaces an object with the given value, but only if there is already a value for that key.")
  
  (put-all-if-present
    [mc key-val-map]
    [mc key-val-map exp]
    "Replaces multiple values")

  (delete
    [mc key]
    "Deletes a key from the cache")
  
  (delete-all [mc keys]  ;; delete-all
    "Deletes multiple keys")

  (incr
    [mc key]
    [mc key by]
    [mc key by default]
    [mc key by default exp]
    "Increments a long int counter associated with the key. 'by' is
    the amount to increment (1 if not specified.) If default is not
    specified, counter must exist.")
  
  (decr
    [mc key]
    [mc key by]
    [mc key by default]
    [mc key by default exp]
    "Increments a long int counter associated with the key. 'by' is
    the amount to increment (1 if not specified.) If default is not
    specified, counter must exist.")

  (fetch
    [mc key]
    "Gets value for key.")

  (fetch-all
    [mc keys]
    "Gets values for keys. Returns a map of keys to values, omitting
    keys that are not in the cache.")

  (clear [mc] "clear the cache")

  )

(defmacro with-cache
  "key is a string, value-fn is a function. Returns keyed value from cache;
   if not found, uses value-fn to obtain the value and adds it to cache
   before returning."
  ([cache key value-fn]
      `(if-let [cached-val# (fetch ~cache ~key)]
         cached-val#
         (let [val# (~value-fn ~key)]
           (if (nil? val#)
             nil
             (if (.get (put-if-absent ~cache ~key val#))
               val# 
               (fetch ~cache ~key))))))
  ([cache key value-fn exp]
      `(if-let [cached-val# (fetch ~cache ~key)]
         cached-val#
         (let [val# (~value-fn ~key)]
           (if (nil? val#)
             nil
             (if (.get (put-if-absent ~cache ~key val# ~exp))
               val#
               (fetch ~cache ~key)))))))

(defn cache-fetch
 ([cache id query-fn key-fn] (cache-fetch cache id query-fn key-fn (default-exp cache)))
 ([cache id query-fn key-fn exp]
    "To be used in conjunction with a persistent storage api. 'id' is
    a unique identifier (e.g. a primary key) for a persisted object.
    'query-fn' is a function accepting an id as argument, and
    returning the persisted object or nil. 'key-fn' is a function which
    accepts an id and returns a corresponding cache key. This function
    will first attempt to locate the object in cache; if not found, it
    uses query-fn to get the object and caches it before returning.
    Returns nil if object is not found at all."
    (if-let [cached-obj (fetch cache (key-fn id))]
      cached-obj
      (when-let [obj (query-fn id)]
        (put-if-absent cache (key-fn id) obj exp)
        obj))))

(defn- remove-nil-vals [map]
  (reduce #(if (nil? (second %2)) %1 (assoc %1 (first %2) (second %2))) {} map))

(defn cache-fetch-all
  [cache ids query-fn key-fn]
  "This is similar to cache-fetch, except it handles a sequence of
     ids. Returns a sequence containing the resolved objects, or nil for
     not-found objects, in the same order as the original sequence of
     ids."
  (letfn [(add-to-cache
            [cache id2val key-fn]
            (doseq [[id val] id2val]
              (put-if-absent cache (key-fn id) val)))
          (from-cache
            [cache ids key-fn]
            (let [keys (map key-fn ids)]
              (clojure.set/rename-keys (fetch-all cache keys) (zipmap keys ids))))]
    (let [fromcache (from-cache cache ids key-fn)
          ids-to-query (remove #(contains? fromcache %) ids)
          fromquery (if (empty? ids-to-query) {} (remove-nil-vals (query-fn ids-to-query)))]
      (add-to-cache cache fromquery key-fn)
      (merge fromcache fromquery))))


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

  (incr
    ([mc key]
       (.. mc (incr key 1)))
    ([mc key by]
       (.. mc (incr key by)))
    ([mc key by default]
       (.. mc (incr key by default)))
    ([mc key by default exp]
       (.. mc (incr key by default exp))))
  
  (decr
    ([mc key]
       (.. mc (decr key 1)))
    ([mc key by]
       (.. mc (decr key by)))
    ([mc key by default]
       (.. mc (decr key by default)))
    ([mc key by default exp]
       (.. mc (decr key by default exp))))
  
  (fetch [mc key]
    (.. mc (get key)))

  (fetch-all [mc keys]
    (into {} (.. mc (getBulk keys)))) 

  (clear [mc] (.. mc (flush)))

  )







(defcache MemcachedCache [mc exp]
  CacheProtocol
  (lookup
   [this e]
   (.. mc (get e)))

  (has?    [this e]
    "Memcached does provide ability to test existence of a key without
     fetching the entire value, so this function has no performance
     advantage over lookup."
    (not (nil? (.. mc (get e)))))
  
  (hit     [this e]
   "Is meant to be called if the cache is determined to contain a value
   associated with `e`"
    this)
  
  (miss    [this e ret]
   "Is meant to be called if the cache is determined to **not** contain a
   value associated with `e`"
   (when-not (nil? ret) (put mc e ret))
   this)
  
  (evict  [this e]
    "Removes an entry from the cache"
    (delete mc e)
    this)
  
  (seed    [this base]
   "Is used to signal that the cache should be created with a seed.
   The contract is that said cache should return an instance of its
   own type."
   (put-all mc base)
   this)

  Object
  (toString [this] (str "MemcachedCache: " mc))

  Memcache
  (default-exp [this] DEFAULT-EXP)
  
  (put-if-absent
   [this key value exp]
   (.. mc (add key exp value)))
  
  (put-if-absent
   [this key value]
   (.. mc (add key DEFAULT-EXP value)))
  
  (put-all-if-absent
   [this key-val-map]
   (put-all-if-absent this key-val-map DEFAULT-EXP))
  
  (put-all-if-absent
   [this key-val-map exp]
   (cache-update-multi put-if-absent this key-val-map exp))
  
  (put
   [this key value]
   (.. mc (set key DEFAULT-EXP value)))
  (put
   [this key value exp]
   (.. mc (set key exp value)))
  
  (put-all
   [this key-val-map]
   (put-all this key-val-map DEFAULT-EXP))
  
  (put-all
   [this key-val-map exp]
   (cache-update-multi put this key-val-map exp))

  (put-if-present
   [this key value]
   (put-if-present this key value DEFAULT-EXP))

  (put-if-present
   [this key value exp]
   (.. mc (replace key exp value)))
  
  (put-all-if-present
   [this key-val-map]
   (put-all-if-present this key-val-map DEFAULT-EXP))

  (put-all-if-present
   [this key-val-map exp]
   (cache-update-multi put-if-present this key-val-map exp))

  (delete
   [this key]
   (.. mc (delete key)))
  
  (delete-all
   [this keys]
   (doall (map #(delete this %) keys)))

  (incr [this key]
        (.. mc (incr key 1)))
  (incr [this key by]
        (.. mc (incr key by)))
  (incr [this key by default]
        (.. mc (incr key by default)))
  (incr [this key by default exp]
        (.. mc (incr key by default exp)))
  
  (decr [this key]
        (.. mc (decr key 1)))
  (decr [this key by]
        (.. mc (decr key by)))
  (decr [this key by default]
        (.. mc (decr key by default)))
  (decr [this key by default exp]
        (.. mc (decr key by default exp)))
  
  (fetch
   [this key]
   (.. mc (get key)))
  
  (fetch-all
   [this keys]
   (into {} (.. mc (getBulk keys)))) 

  (clear [this] (.. mc (flush)))

  )


(defn make-memcached-cache
  ([mc] (make-memcached-cache mc (* 60 60 24 30)) )
  ([mc expiration] (MemcachedCache. mc expiration)))

