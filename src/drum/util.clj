(ns drum.util
  (:require [clj-memory-meter.core :refer [measure]])
  (:import [javax.xml.bind DatatypeConverter]))


(defn file-size
  "Returns the file size in bytes."
  [full-path]
  (.length (clojure.java.io/file full-path)))

(defn size-in-bytes
  "Returns the number of bytes used to represent x in memory."
  [x]
  (measure x :bytes true))


(defn hash->bytes
  "Returns byte array of the hex-string."
  [hex]
  (DatatypeConverter/parseHexBinary hex))

(defn gen-kv-buffer
  "Sets the aux field of the buffer of drumEntries to nil."
  [buffer]
  (into [] (map #(assoc % :aux nil) buffer)))

(defn gen-aux-buffer
  "Sets the key and value fields of the buffers of drumEntries to nil
  and removes those that are not check operations."
  [buffer]
  (into [] (map #(:aux %) (filter #(:check %) buffer))))

(defn need-to-merge?
  "Sends the merge action to the merge agent if either files are full and merge is not taking place."
  [kv-file aux-file max-size merging]
  (and
    (> (max (file-size kv-file) (file-size aux-file)) max-size)
    (compare-and-set! merging false true)))

(defn hash->drum-index
  "Return the drum bucket index for the provided hash value based on
  the first byte value."
  [n hex]
  (let [byteval (read-string (str "0x" (.substring hex 0 2)))]
    (dec (int (Math/ceil (/ byteval (/ 256 n)))))))