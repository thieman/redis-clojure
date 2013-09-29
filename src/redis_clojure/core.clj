(ns redis-clojure.core
  (:import (java.net Socket)
           (java.io PrintWriter InputStreamReader BufferedReader)))

(def host "127.0.0.1")
(def port (int 6379))

(declare conn-handler)

(defn connect [host port]
  (let [socket (Socket. host port)
        in (BufferedReader. (InputStreamReader. (.getInputStream socket)))
        out (PrintWriter. (.getOutputStream socket))
        conn (ref {:in in :out out})]
    (doto (Thread. #(conn-handler conn)) (.start))
    conn))

(defn write [conn msg]
  (doto (:out @conn)
    (.println (str msg "\r"))
    (.flush)))

(defn conn-handler [conn]
  (while (nil? (:exit @conn))
    (let [msg (.readLine (:in @conn))]
      (println msg)
      (cond
       (re-find #"^ERROR :Closing Link:" msg)
       (dosync (alter conn merge {:exit true}))
       (re-find #"^PING" msg)
       (write conn (str "PONG " (re-find #":.*" msg)))))))

(defn -main []
  (def conn (connect host port))
  (write conn "INFO"))
