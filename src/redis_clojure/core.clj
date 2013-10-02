(ns redis-clojure.core
  (:require [redis-clojure.protocol :as protocol]))

(def connect protocol/connect)
(def request-string protocol/request-string)
(def synchronous-request protocol/synchronous-request)

(def bound-conn (atom nil))

(defn bind-connection [bind-conn] (reset! bound-conn bind-conn))

(defn is-ref? [r] (= (type r) clojure.lang.Ref))

(defn get-connection []
  (let [conn @bound-conn]
    (if (nil? conn)
      (throw (Exception. "No connection has been bound."))
      conn)))

(defn redis-command [name]
  (let [f (fn f [args]
            (let [head (first args)]
              (if (and (is-ref? head) (:connection @head))
                (synchronous-request head (apply request-string name (next args)))
                (f (apply vector (get-connection) (vec args))))))]
    (intern *ns* (symbol name) f)))

(redis-command "info")
(redis-command "llen")


;; (defn info
;;   ([] (info (get-connection)))
;;   ([conn] (synchronous-request conn (request-string "info"))))

;; (defn redis-get
;;   ([key] (redis-get (get-connection) key))
;;   ([conn key] (synchronous-request conn (request-string "get" key))))

;; (defn redis-set
;;   ([key value] (redis-set (get-connection) key value))
;;   ([conn key value] (synchronous-request conn (request-string "set" key value))))

;; (defn llen
;;   ([key] (llen (get-connection) key))
;;   ([conn key] (synchronous-request conn (request-string "llen" key))))

;; (defn lpop
;;   ([key] (lpop (get-connection) key))
;;   ([conn key] (synchronous-request conn (request-string "lpop" key))))

;; (defn lrange
;;   ([key start stop] (lrange (get-connection) key start stop))
;;   ([conn key start stop] (synchronous-request conn (request-string "lrange" key start stop))))

;; (defn lpush
;;   ([key value] (lpush (get-connection) key value))
;;   ([conn key value] (synchronous-request conn (request-string "lpush" key value))))
