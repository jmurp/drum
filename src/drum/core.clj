(ns drum.core
  (:require [drum.util :refer :all]
            [clojure.java.io :as io])
  (:import [java.util.concurrent.locks ReentrantLock]
           [drum KeyComparator DrumManager]
           [com.sleepycat.je Cursor DatabaseEntry]))

(defn drum-entry
  [{:keys [key value aux check update order result]}]
  (->drumEntry key value aux check update order result))

(defn read-buffer-from-file
  "Reads a buffer back into memory as a sequence."
  [filename]
  (with-open [reader (io/reader filename)]
    (reduce #(concat %1 (read-string %2)) [] (line-seq reader))))

(defn get-sorted-bucket-buffer
  "Returns the sorted bucket buffer from file."
  [kv-file]
  (let [buffer (read-buffer-from-file kv-file)
        n (count buffer)
        key-comp (KeyComparator.)]
    (sort
      #(.compare key-comp (hash->bytes (:key %1)) (hash->bytes (:key %2)))
      (map #(assoc %1 :order %2) buffer (range n)))))

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

(defn gen-kv-buffer
  "Sets the aux field of the buffer of drumEntries to nil."
  [buffer]
  (into [] (map #(assoc % :aux nil) buffer)))

(defn gen-aux-buffer
  "Sets the key and value fields of the buffers of drumEntries to nil
  and removes those that are not check operations."
  [buffer]
  (into [] (map #(:aux %) (filter #(:check %) buffer))))

(defn transfer-overflow
  "Transfers contents of the overflow files into the normal files.
  Truncates the overflow files and returns the new offsets for writing."
  [kv-file aux-file]
  (let [kv-overflow (read-buffer-from-file (str kv-file "_OF"))
        aux-overflow (read-buffer-from-file (str aux-file "_OF"))]
    (spit kv-file (with-out-str (pr kv-overflow)) :append true)
    (spit kv-file "\n" :append true)
    (spit aux-file (with-out-str (pr aux-overflow)) :append true)
    (spit aux-file "\n" :append true)
    (set-file-size (str kv-file "_OF") 0)
    (set-file-size (str aux-file "_OF") 0)))

(defn write-buffers
  "Writes the appropriate buffers to their files."
  [kv-file aux-file buffer]
  (spit kv-file (with-out-str (pr (gen-kv-buffer buffer))) :append true)
  (spit kv-file "\n" :append true)
  (spit aux-file (with-out-str (pr (gen-aux-buffer buffer))) :append true)
  (spit aux-file "\n" :append true))

(defn write-buffers-overflow
  "Writes the buffers to their overflow files, which are not allocated."
  [kv-file aux-file buffer]
  (spit (str kv-file "_OF") (with-out-str (pr (gen-kv-buffer buffer))) :append true)
  (spit (str kv-file "_OF") "\n" :append true)
  (spit (str aux-file "_OF") (with-out-str (pr (gen-aux-buffer buffer))) :append true)
  (spit (str aux-file "_OF") "\n" :append true))

(defn need-to-merge?
  "Sends the merge action to the merge agent if either files are full and merge is not taking place."
  [kv-file aux-file max-size merging]
  (and
    (> (max (file-size kv-file) (file-size aux-file)) max-size)
    (compare-and-set! merging false true)))

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

(defn hash->drum-index
  "Return the drum bucket index for the provided hash value based on
  the first byte value."
  [n hex]
  (let [byteval (read-string (str "0x" (.substring hex 0 2)))]
    (dec (int (Math/ceil (/ byteval (/ 256 n)))))))

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