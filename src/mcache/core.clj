(ns mcache.core
  "Protocol for memory cache, and spymemcached implementation"
  (:use [clojure.core.cache :only (defcache)]
        [mcache.util :only (cache-update-multi)])
  (:require [clojure.set]
            [clojure.core.cache])
  (:import [clojure.core.cache CacheProtocol]))

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

  (fetch
    [mc key]
    "Gets value for key.")

  (fetch-all
    [mc keys]
    "Gets values for keys. Returns a map of keys to values, omitting
    keys that are not in the cache.")

  (clear [mc] "clear the cache"))

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

(defn cache-backed-lookup
 ([cache id query-fn key-fn] (cache-backed-lookup cache id query-fn key-fn (default-exp cache)))
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

(defn cache-backed-lookup-all
  [cache ids query-fn key-fn]
  "This is similar to cache-backed-lookup, except it handles a sequence of
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
   (put-if-absent mc key value exp))
  
  (put-if-absent
   [this key value]
   (put-if-absent mc key value))
  
  (put-all-if-absent
   [this key-val-map]
   (put-all-if-absent mc key-val-map))
  
  (put-all-if-absent
   [this key-val-map exp]
   (cache-update-multi put-if-absent this key-val-map exp))
  
  (put
   [this key value]
   (put mc key value))
  (put
   [this key value exp]
   (put mc key value exp))
  
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
   (put-if-present mc key value exp))
  
  (put-all-if-present
   [this key-val-map]
   (put-all-if-present this key-val-map DEFAULT-EXP))

  (put-all-if-present
   [this key-val-map exp]
   (cache-update-multi put-if-present this key-val-map exp))

  (delete
   [this key]
   (delete mc key))
  
  (delete-all
   [this keys]
   (doall (map #(delete this %) keys)))

  (fetch
   [this key]
   (fetch mc key))
  
  (fetch-all
   [this keys]
   (fetch-all mc keys)) 

  (clear
   [this]
   (clear mc)))


(defn memcached-cache-factory
  "Returns a memcached-based cache using the given memcached client object"
  ([mc-client] (memcached-cache-factory mc-client (* 60 60 24 30)) )
  ([mc-client expiration] (MemcachedCache. mc-client expiration)))

