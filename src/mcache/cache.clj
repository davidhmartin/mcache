(ns mcache.cache
  "Protocol for memory cache"
  (:use alex-and-georges.debug-repl)
  )


(defprotocol Memcache
  "This is a protocol for memcached and similar cache clients. The
functions which add to the cache include an 'exp' parameter, which is
the expiration in seconds. If omitted, a default expiration, specific
to the protocol implementation, is used."

  (default-exp [mc])
  
  (cache-add
    [mc key value]
    [mc key value exp]
    "Add a key/value to the cache, if the key is not already there."
    )
  
  (cache-add-all
    [mc key-val-map]
    [mc key-val-map exp]
    "Add multiple keys/values to the cache, if not already there.")
  
  (cache-set
    [mc key value]
    [mc key value exp]
    "Set a key/value. If a value is already cached with that key, it is replaced.")
  
  (cache-set-all
    [mc key-val-map]
    [mc key-val-map exp]
    "Sets multiple keys/values, overwriting existing values if any.")
  
  (cache-replace
    [mc key value]
    [mc key value exp]
    "Replaces an object with the given value, but only if there is already a value for that key.")
  
  (cache-replace-all
    [mc key-val-map]
    [mc key-val-map exp]
    "Replaces multiple values")

  (cache-delete
    [mc key]
    "Deletes a key from the cache")
  
  (cache-delete-all [mc keys]
    "Deletes multiple keys")

  (cache-incr
    [mc key]
    [mc key by]
    [mc key by default]
    [mc key by default exp]
    "Increments a long int counter associated with the key. 'by' is
    the amount to increment (1 if not specified.) If default is not
    specified, counter must exist.")
  
  (cache-decr
    [mc key]
    [mc key by]
    [mc key by default]
    [mc key by default exp]
    "Increments a long int counter associated with the key. 'by' is
    the amount to increment (1 if not specified.) If default is not
    specified, counter must exist.")

  (cache-get
    [mc key]
    "Gets value for key.")

  (cache-get-all
    [mc keys]
    "Gets values for keys. Returns a map of keys to values, omitting
    keys that are not in the cache.")

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
    
    (if-let [cached-obj (cache-get mc (key-fn id))]
      cached-obj
      (when-let [obj (query-fn id)]
        (cache-add mc (key-fn id) obj exp)
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
                (cache-add mc (key-fn id) val exp)))
            (from-cache
              [mc ids key-fn]
              (let [keys (map key-fn ids)]
                (clojure.set/rename-keys (cache-get-all mc keys) (zipmap keys ids))))]
      (let [fromcache (from-cache mc ids key-fn)
            fromquery (remove-nil-vals (query-fn (remove #(contains? fromcache %) ids)))]
        (add-to-cache mc fromquery key-fn exp)
        (merge fromcache fromquery)))))

