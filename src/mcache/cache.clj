(ns mcache.cache
  "Protocol for memory cache, and spymemcached implementation"
  (:use [clojure.core.cache :only (defcache)])
  (:require [clojure.set]
            [clojure.core.cache]
            )
  (:import [clojure.core.cache CacheProtocol])
  )

(defprotocol Memcache
  "This is a protocol for memcached and similar cache clients.

   The functions which insert values into the cache may optionally
   accept an expiration, the semantics of which depend on the
   protocol implementation."


  (default-exp [mc]
    "Return the default expiration")
  
  (put-if-absent 
    [mc key value]
    [mc key value exp]
    "Add a key/value to the cache, if the key is not already there."
    )
  
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
    keys that are not in the cache."))


(defmacro with-cache
  "key is a string, value-fn is a function. Returns keyed value from cache;
   if not found, uses value-fn to obtain the value and adds it to cache
   before returning."
  ([mc key value-fn]
      `(if-let [cached-val# (fetch ~mc ~key)]
         cached-val#
         (let [val# ~value-fn]
           (put-if-absent ~mc ~key val#)
           val#)))
  ([mc key value-fn exp]
      `(if-let [cached-val# (fetch ~mc ~key)]
         cached-val#
         (let [val# ~value-fn]
           (put-if-absent ~mc ~key val# ~exp)
           val#)))
  )

(defn cache-fetch
 ([mc id query-fn key-fn] (cache-fetch mc id query-fn key-fn (default-exp mc)))
 ([mc id query-fn key-fn exp]
    "To be used in conjunction with a persistent storage api. 'id' is
    a unique identifier (e.g. a primary key) for a persisted object.
    'query-fn' is a function accepting an id as argument, and
    returning the persisted object or nil. 'key-fn' is a function which
    accepts an id and returns a corresponding cache key. This function
    will first attempt to locate the object in cache; if not found, it
    uses query-fn to get the object and caches it before returning.
    Returns nil if object is not found at all."
    (if-let [cached-obj (fetch mc (key-fn id))]
      cached-obj
      (when-let [obj (query-fn id)]
        (put-if-absent mc (key-fn id) obj exp)
        obj))))

(defn- remove-nil-vals [map]
  (reduce #(if (nil? (second %2)) %1 (assoc %1 (first %2) (second %2))) {} map))

(defn cache-fetch-all
 ([mc ids query-fn key-fn] (cache-fetch-all mc ids query-fn key-fn (default-exp mc)))
 ([mc ids query-fn key-fn exp]
    "This is similar to cached-fetch, except it handles a sequence of
     ids. Returns a sequence containing the resolved objects, or nil for
     not-found objects, in the same order as the original sequence of
     ids."
    (letfn [(add-to-cache
              [mc id2val key-fn exp]
              (doseq [[id val] id2val]
                (put-if-absent mc (key-fn id) val exp)))
            (from-cache
              [mc ids key-fn]
              (let [keys (map key-fn ids)]
                (clojure.set/rename-keys (fetch-all mc keys) (zipmap keys ids))))]
      (let [fromcache (from-cache mc ids key-fn)
            ids-to-query (remove #(contains? fromcache %) ids)
            fromquery (if (empty? ids-to-query) {} (remove-nil-vals (query-fn ids-to-query)))]
        (add-to-cache mc fromquery key-fn exp)
        (merge fromcache fromquery)))))





(def EXP (* 60 60 24 30))

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

  (default-exp [mc] EXP)
  
  (put-if-absent
    ([mc key value]
       (put-if-absent mc key value EXP))
    ([mc key value exp]
       (.. mc (add key exp value))))
  
  (put-all-if-absent
    ([mc key-val-map]
       (put-all-if-absent mc key-val-map EXP))
    ([mc key-val-map exp]
       (cache-update-multi put-if-absent mc key-val-map exp)))
  
  (put
    ([mc key value]
       (.. mc (set key EXP value)))
    ([mc key value exp]
       (.. mc (set key exp value))))
  
  (put-all
    ([mc key-val-map] (put-all mc key-val-map EXP))
    ([mc key-val-map exp]
       (cache-update-multi put mc key-val-map exp)))
  
  (put-if-present
    ([mc key value] (put-if-present mc key value EXP))
    ([mc key value exp]
       (.. mc (replace key exp value))))
  
  (put-all-if-present
    ([mc key-val-map] (put-all-if-present mc key-val-map EXP))
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

  )



(defcache MemcachedCache [mc]
  CacheProtocol
  (lookup
   [this e]
   (fetch mc e))

  (has?    [this e]
    "Memcached does provide ability to test existence of a key without
     fetching the entire value, so this function has no performance
     advantage over lookup."
    (not (nil? (fetch mc e))))
  
  (hit     [this e]
   "Is meant to be called if the cache is determined to contain a value
   associated with `e`"
    this)
  
  (miss    [this e ret]
   "Is meant to be called if the cache is determined to **not** contain a
   value associated with `e`"
   (put mc e ret)
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
   this
   )


  ;; Memcache
  ;; (default-exp [mc] EXP)
  
  ;; (put-if-absent
  ;;   ([mc key value]
  ;;      (put-if-absent mc key value EXP))
  ;;   ([mc key value exp]
  ;;      (.. mc (add key exp value))))
  
  ;; (put-all-if-absent
  ;;   ([mc key-val-map]
  ;;      (put-all-if-absent mc key-val-map EXP))
  ;;   ([mc key-val-map exp]
  ;;      (cache-update-multi put-if-absent mc key-val-map exp)))
  
  ;; (put
  ;;   ([mc key value]
  ;;      (.. mc (set key EXP value)))
  ;;   ([mc key value exp]
  ;;      (.. mc (set key exp value))))
  
  ;; (put-all
  ;;   ([mc key-val-map] (put-all mc key-val-map EXP))
  ;;   ([mc key-val-map exp]
  ;;      (cache-update-multi put mc key-val-map exp)))
  
  ;; (put-if-present
  ;;   ([mc key value] (put-if-present mc key value EXP))
  ;;   ([mc key value exp]
  ;;      (.. mc (replace key exp value))))
  
  ;; (put-all-if-present
  ;;   ([mc key-val-map] (put-all-if-present mc key-val-map EXP))
  ;;   ([mc key-val-map exp]
  ;;      (cache-update-multi put-if-present mc key-val-map exp)))

  ;; (delete [mc key]
  ;;   (.. mc (delete key)))
  
  ;; (delete-all [mc keys]
  ;;   (doall (map #(delete mc %) keys)))

  ;; (incr
  ;;   ([mc key]
  ;;      (.. mc (incr key 1)))
  ;;   ([mc key by]
  ;;      (.. mc (incr key by)))
  ;;   ([mc key by default]
  ;;      (.. mc (incr key by default)))
  ;;   ([mc key by default exp]
  ;;      (.. mc (incr key by default exp))))
  
  ;; (decr
  ;;   ([mc key]
  ;;      (.. mc (decr key 1)))
  ;;   ([mc key by]
  ;;      (.. mc (decr key by)))
  ;;   ([mc key by default]
  ;;      (.. mc (decr key by default)))
  ;;   ([mc key by default exp]
  ;;      (.. mc (decr key by default exp))))
  
  ;; (fetch [mc key]
  ;;   (.. mc (get key)))

  ;; (fetch-all [mc keys]
  ;;   (into {} (.. mc (getBulk keys)))) 


  )