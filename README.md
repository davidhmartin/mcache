# mcache

This is a clojure mcached client library, providing the following:

* A protocol, "mcache.core.Memcache", defining basic memcached-related functions for setting, getting, and removing to/from memcached

* Extends the [spymemcached](http://code.google.com/p/spymemcached/) memcached client object with the Memcache protocol.

* Provides some higher-level macros and functions geared towards using memcached with a persistent store; these functions are implemented entirely in terms of the Memcache protocol.

* Integrates with the [clojure.core.cache](https://github.com/clojure/core.cache) caching library


## Usage

Add this to your project.clj:

    [mcache "0.2.0"]

### Example usage:

    (ns mcache.test.cache
        (:use [mcache.core]
              [mcache.spy])
        (:import [clojure.core.cache CacheProtocol]
                 [mcache.core MemcachedCache]))

    ;; construct the client
    (def mc (net.spy.memcached.MemcachedClient. (list (java.net.InetSocketAddress. "127.0.0.1" 11211))))

    ;; Example of basic put/fetch/delete usage:
    (put mc "foo" "this is a cached value")
    ;...
    (fetch mc "foo")  ;; returns "this is a cached value"
    ;...
    (delete mc "foo")

    ;; You can do bulk sets by passing a map of key->value:
    (put-all mc {"foo" "this is foo value" 
                 "bar" "this is bar value" 
                 "anumber" 42})

    ;; You can do bulk gets, passing a sequence of keys:
    (fetch-all mc ["foo" "bar" "anumber"])

    ;; And bulk deletes:
    (delete-all mc ["foo" "anumber"])


    ;; The Memcache protocol is not meant to wrap all functionality. 
    ;; Use Java interop for cache operations not covered in the protocol:

    (.. mc (incr "aKey")) ;; increments and returns a value
    (.. mc (incr "anotherKey" 2 0)) ;; increments by two, default 0
    (.. mc (getStats))  ;; get stats from all clients
    (.. mc (cas "aKey" casId value)) ;; check-and-set


### Expirations: 

  Operations that associate a value with a key (add,
  set, replace, etc) take an optional 'exp' argument, representing
  expiration in seconds. If omitted, a default is used. In the case of
  the spymemcached implementation, the default expiration is 30 days.
  The expiration value follows the memcached protocol: the expiration
  is a long representing seconds. If <= 30 days, it represents
  time-to-live; if > 30 days, it is assumed to be an expiration date,
  in seconds since Jan 1 1970.


### Higher-level functions:

with-cache macro

The library provides a "with-cache" macro, accepting a key and an sexpr to produce a value in the case where the cache does not contain the key.

It also provides two functions supporting cached-backed keyed lookup of data from a persistent store: "cache-backed-lookup" and "cache-backed-lookup-all". 

See the docs for these functions for details.

### [clojure.core.cache](https://github.com/clojure/core.cache) integration

You can create a CacheProtocol object by constructing an instance of
mcache.cache.MemcachedCache, passing it an instance of an object which
extends the Memcache protocol. For example:

    (MemcachedCache. 
      (net.spy.memcached.MemcachedClient. (list (java.net.InetSocketAddress. "127.0.0.1" 11211))))

There is also a factory function which does the same:

    (memcached-cache-factory
      (net.spy.memcached.MemcachedClient. (list (java.net.InetSocketAddress. "127.0.0.1" 11211))))

The resulting object extends both the clojure.core.cache.CacheProtocol
protocol, and the mcache.cache.Memcache protocol. Any functions
written against the Memcache protocol should work seamlessly against
either a MemcachedCache object or a net.spy.memcached.MemcachedClient
instance. Likewise, this integration with clojure.core.cache should
allow the use of memcached for functions that talk to CacheProtocol.


## License

Copyright (C) 2012 David H. Martin

Distributed under the Eclipse Public License, the same as Clojure.
