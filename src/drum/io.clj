(ns drum.io
  (:require [clojure.java.io :as io]
            [drum.util :refer :all])
  (:import [java.io RandomAccessFile]
           [drum KeyComparator]))

(defn set-file-size
  "Uses RandomAccessFile to set the file size."
  [filename size]
  (let [raf (RandomAccessFile. (io/file filename) "rwd")]
    (.setLength raf size)
    (.close raf)))

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