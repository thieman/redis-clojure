(ns redis-clojure.core
  (:import (java.net Socket)
           (java.io PrintWriter InputStreamReader BufferedReader)))

(def debug true)

(declare conn-handler append-status-reply append-error-reply append-integer-reply
         append-bulk-reply append-multi-bulk-reply append-complete-response)

(def responses (atom []))
(def ^:private current-response (atom []))
(def ^:private response-type (atom :none))

(def response-types {\+ :status \- :error \: :integer \$ :bulk \* :multi-bulk})

(defn connect [host port]
  (let [socket (Socket. host port)
        in (BufferedReader. (InputStreamReader. (.getInputStream socket)))
        out (PrintWriter. (.getOutputStream socket))
        conn (ref {:in in :out out})]
    (doto (Thread. #(conn-handler conn)) (.start))
    conn))

(defn- write [conn msg]
  (doto (:out @conn)
    (.println (str msg "\r"))
    (.flush)))

(defn request-string [& args]
  (let [total-args (count args)
        specify-arg (fn [coll arg] (conj coll (str "$" (count (str arg))) arg))
        args-list (reduce specify-arg [] args)]
    (clojure.string/join "\r\n" (flatten (conj [] (str "*" total-args) args-list "")))))

(defn- spec-int-from-response-part [response-part]
  (-> response-part
      (next)
      ((partial drop-last 2))
      (clojure.string/join)
      (Integer/parseInt)))

(defn- bulk-is-complete? [response-parts]
  (let [response-size (spec-int-from-response-part (first response-parts))
        content (clojure.string/join (next response-parts))]
    (= (+ 2 response-size) (count content))))

(defn- consolidate-multi-bulk-args [args]
  (loop [args args
         result []
         expected-size nil]
    (if (empty? args)
      result
      (let [x (first args)
            head (first x)]
        (if (or (= \* head) (= \$ head))
          (recur (next args) (conj result x) (spec-int-from-response-part x))
          (if (= (+ 2 expected-size) (count x))
            (recur (next args) (conj result x) nil)
            (if (or (= \* (first (last result))) (= \$ (first (last result))))
              (recur (next args) (conj result x) expected-size)
              (recur (next args) (conj (vec (butlast result)) (clojure.string/join [(last result) x])) expected-size))))))))

(defn- gen-arg-pairs [multi-args]
  (loop [result []
         args (next multi-args)]
    (if (zero? (count args))
      result
      (let [current (if (<= 2 (count result)) (vec (butlast result)) [])]
        (if (zero? (rem (count (last result)) 2))
          (recur (conj result [(first args)]) (next args))
          (recur (conj current (conj (last result) (first args))) (next args)))))))

(defn- multi-bulk-is-complete? [response-parts]
  (let [total-args (spec-int-from-response-part (first response-parts))
        consolidated-args (consolidate-multi-bulk-args response-parts)
        arg-pairs (gen-arg-pairs consolidated-args)]
    (and (= (+ 1 (* 2 total-args)) (count consolidated-args))
         (every? true? (map bulk-is-complete? arg-pairs)))))

(defn- is-complete? [response-parts]
  (cond
   (contains? #{:status :error :integer} @response-type)
   true
   (= :bulk @response-type)
   (bulk-is-complete? response-parts)
   (= :multi-bulk @response-type)
   (multi-bulk-is-complete? response-parts)))

(defn synchronous-request [conn msg]
  ;; cannot be run in the middle of a pipelined request
  (assert (zero? (count @responses)))
  (let [responses-size (count @responses)]
    (write conn msg)
    (while (= responses-size (count @responses))
      (Thread/sleep 1))
    (let [result (first @responses)]
      (reset! responses [])
      result)))

(defn- finalize-current-response []
  (let [current @current-response
        head (-> (drop-last 2 (next (first current)))
                 (clojure.string/join))]
    (condp = @response-type
      :status (append-status-reply head)
      :error (append-error-reply head)
      :integer (append-integer-reply head)
      :bulk (append-bulk-reply current)
      :multi-bulk (append-multi-bulk-reply current))))

(defn- append-status-reply [msg]
  (append-complete-response {:type :status
                             :value (msg)}))

(defn- append-error-reply [msg]
  (append-complete-response {:type :error
                             :value (msg)}))

(defn- append-integer-reply [msg]
  (append-complete-response {:type :integer
                             :value (Integer/parseInt msg)}))

(defn- append-bulk-reply [msg]
  (append-complete-response {:type :bulk
                             :value (map (comp clojure.string/join (partial drop-last 2)) (next msg))}))

(defn- append-multi-bulk-reply [msg]
  (let [value (map (comp clojure.string/join (partial drop-last 2)) (next msg))]
    (append-complete-response {:type :multi-bulk
                               :value (filter (complement nil?) (map-indexed #(if (odd? %1) %2 nil) value))})))

(defn- append-response-part [msg]
  (swap! current-response #(conj % msg))
  (when (= @response-type :none)
    (reset! response-type (get response-types (first msg))))
  (when (is-complete? @current-response)
    (finalize-current-response)))

(defn- append-complete-response [response]
  (swap! responses #(conj % response))
  (reset! response-type :none)
  (reset! current-response []))

(defn- conn-handler [conn]
  (loop [msg ""]
    (let [in-char (char (.read (:in @conn)))
          curr-msg (str msg in-char)]
      (when (nil? (:exit @conn))
        (if (re-find #"\r\n" curr-msg)
          (if (= curr-msg "PING\r\n")
            (write conn "PONG")
            (do
              (append-response-part curr-msg)
              (recur "")))
          (recur curr-msg))))))

(defn -main []
  (let [conn (connect "127.0.0.1" 6379)]
    (println (synchronous-request conn (request-string "llen" "bacon")))
    (println (synchronous-request conn (request-string "lrange" "bacon" 0 1)))))
    ;; (println (synchronous-request conn (request-string "info")))))
