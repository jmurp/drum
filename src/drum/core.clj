(ns drum.core
  (:require [drum.util :refer :all]
            [drum.io :refer :all]
            [clojure.java.io :as io])
  (:import [java.util.concurrent.locks ReentrantLock]
           [drum DrumManager]
           [com.sleepycat.je Cursor DatabaseEntry]
           [java.security MessageDigest]
           [java.util Arrays]
           [javax.xml.bind DatatypeConverter]))

(def MD (MessageDigest/getInstance "SHA-256"))
(defn eight-byte-hash
  "Returns an eight byte hash of the input string in hex-string format."
  [input]
  (let [bytes (.digest MD (.getBytes input))
        shorter-bytes (Arrays/copyOfRange bytes 0 8)]
    (DatatypeConverter/printHexBinary shorter-bytes)))

(defrecord drumEntry [key value aux check update order result])

(defn drum-entry
  [{:keys [key value aux check update order result]}]
  (->drumEntry key value aux check update order result))

(defn perform-dispatch
  "Calls the provided dispatch-fn on each map which is a merge of the original
  aux and the merge-results with keys (:key :value :u :c :o :r :aux).
  Note later the data should be reduced as much as possible to what is needed."
  [aux-file merge-results dispatch-fn]
  (doall
    (map dispatch-fn
         (map #(assoc %1 :aux %2)
              (sort-by :order merge-results)
              (read-buffer-from-file aux-file)))))

(defn single-merge
  "Merges the entry with the database using the cursor. The provided overwrite-fn
  is called when an update is performed on an existing key. The provided check-fn
  is called on all checks with the newest data or no arg if the key is unique
  and determines the value of the added :r key associated with the entry.
  If it is not a check, returns nil."
  [^Cursor cursor overwrite-fn check-fn entry]
  (if (:update entry)
    (if-let [data (DrumManager/findOnUpdate (:key entry) cursor)]
      (let [new-data (overwrite-fn data (:value entry))]
        (.putCurrent cursor (DatabaseEntry. (.getBytes new-data "UTF-8")))
        (if (:check entry)
          (assoc entry :result (check-fn new-data))))
      (do
        (.put cursor
              (DatabaseEntry. (.getBytes (:key entry) "UTF-8"))
              (DatabaseEntry. (.getBytes (:value entry) "UTF-8")))
        (if (:check entry)
          (assoc entry :result (check-fn)))))
    (if (DrumManager/findOnCheck (:key entry) cursor)
      (assoc entry :result (check-fn (DrumManager/getCurrentValue (:key entry) cursor)))
      (assoc entry :result (check-fn)))))

(defn perform-merge
  "Merges the contents of the bucket buffer with the database."
  [^DrumManager DM bucket-buffer overwrite-fn check-fn]
  (let [cursor (.openCursor DM)
        results (doall (map (partial single-merge cursor overwrite-fn check-fn) bucket-buffer))]
    (.closeCursor DM cursor)
    results))

(defn create-merge-action
  "Returns the merge-action used to merge drum with the database.
  The function returned takes an unused first value representing
  an old agent state and the drum object as its second argument."
  [overwrite-fn check-fn dispatch-fn]
  (fn [_ drum]
    (let [DM (DrumManager. (:path drum) (:name drum))]
      (doseq [index (range (count (:buckets drum)))]
        (let [kv-file (:kv-file @(get (:buckets drum) index))
              aux-file (:aux-file @(get (:buckets drum) index))
              ^ReentrantLock file-lock (get (:file-locks drum) index)]
          (.lock file-lock)
          (let [merge-results (filter #(boolean %) (perform-merge DM (get-sorted-bucket-buffer kv-file) overwrite-fn check-fn))]
            (perform-dispatch aux-file merge-results dispatch-fn)
            (set-file-size kv-file 0)
            (set-file-size aux-file 0)
            (.unlock file-lock)))))
    (swap! (:merging drum) (fn [_] false))))

(defn create-drum-bucket
  "Creates a single drum bucket as an agent."
  [path name max-file-size index]
  (let [kv-file (str path name "_KV_" index)
        aux-file (str path name "_AUX_" index)]
    (set-file-size kv-file 0)
    (set-file-size aux-file 0)
    (agent (assoc {:buffer []}
             :kv-file kv-file
             :aux-file aux-file
             :overflow false))))

(defn create-drum-buckets
  "Creates n drum-buckets."
  [path name n max-file-size]
  (into [] (map (partial create-drum-bucket path name max-file-size) (range n))))

(defn create-drum
  "Creates a DRUM with the given name and n buckets storing files in dir.
  Note that n should be 2 to some power. Merge-method is used in building
  the action sent to the merge-agent for merging drum with the disk cache
  and dispatching the results.  See create-drum-merge-action for more.
  See single-merge for more on overwrite-fn and check-fn.
  See perform-dispatch for more on dispatch-fn."
  [dir name n max-buf-size max-file-size overwrite-fn check-fn dispatch-fn]
  (let [path (if (.endsWith dir "/") dir (str dir "/"))]
    (io/make-parents (str path "blah"))
    (assoc {:merging (atom false)}
      :buckets (create-drum-buckets path name n max-file-size)
      :file-locks (into [] (take n (repeatedly #(ReentrantLock.))))
      :merge-agent (agent nil)
      :merge-action (create-merge-action overwrite-fn check-fn dispatch-fn)
      :path path
      :name name
      :max-buffer-size max-buf-size
      :max-file-size max-file-size)))

(defn action-insert
  "Action for the drum buckets on insertion. This function inserts the key
  into the buffer and writes the buffer to disk if it is full. It checks the file size and
  sends the appropriate action to the merge agent if it is not already merging.
  When the lock cannot be acquired, buffers are written to overflow files."
  [old-bucket entry file-lock drum]
  (let [new-buffer (into [] (conj (:buffer old-bucket) entry))]
    (if-not (> (size-in-bytes new-buffer) (:max-buffer-size drum))
      (assoc old-bucket :buffer new-buffer)
      (let [kv-file (:kv-file old-bucket) aux-file (:aux-file old-bucket)]
        (if (.tryLock file-lock)
          (do
            (when (:overflow old-bucket)
              (transfer-overflow kv-file aux-file))
            (write-buffers kv-file aux-file new-buffer)
            (when (need-to-merge? kv-file aux-file (:max-file-size drum) (:merging drum))
              (send-off (:merge-agent drum) (:merge-action drum) drum))
            (.unlock file-lock)
            (assoc old-bucket :buffer [] :overflow false))
          (do
            (write-buffers-overflow kv-file aux-file new-buffer)
            (assoc old-bucket :buffer [] :overflow true)))))))

(defn insert
  "Sends the drum-entry to the correct agent (based on first byte of hash) for insertion into its buffer."
  [drum entry]
  (let [index (hash->drum-index (count (:buckets drum)) (:key entry))]
    (send
      (get (:buckets drum) index)
      action-insert
      entry
      (get (:file-locks drum) index)
      drum)))