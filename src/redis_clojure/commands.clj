(ns redis-clojure.commands
  (:require [redis-clojure.core :as redis]))

(def bound-conn (atom nil))

(defn bind-connection [bind-conn] (reset! bound-conn bind-conn))

(defn get-connection []
  (let [conn @bound-conn]
    (if (nil? conn)
      (throw (Exception. "No connection has been bound."))
      conn)))

(defn info
  ([] (info (get-connection)))
  ([conn] (redis/synchronous-request conn (redis/request-string "info"))))

(defn get
  ([key] (get (get-connection) key))
  ([conn key] (redis/synchronous-request conn (redis/request-string "get" key))))

(defn set
  ([key value] (set (get-connection) key value))
  ([conn key value] (redis/synchronous-request conn (redis/request-string "set" key value))))

(defn llen
  ([key] (llen (get-connection) key))
  ([conn key] (redis/synchronous-request conn (redis/request-string "llen" key))))

(defn lpop
  ([key] (lpop (get-connection) key))
  ([conn key] (redis/synchronous-request conn (redis/request-string "lpop" key))))

(defn lrange
  ([key start stop] (lrange (get-connection) key start stop))
  ([conn key start stop] (redis/synchronous-request conn (redis/request-string "lrange" key start stop))))

(defn lpush
  ([key value] (lpush (get-connection) key value))
  ([conn key value] (redis/synchronous-request conn (redis/request-string "lpush" key value))))
