(ns drum.util
  (:require [clj-memory-meter.core :refer [measure]]
            [clojure.java.io :as io])
  (:import [java.security MessageDigest]
           [java.util Arrays]
           [javax.xml.bind DatatypeConverter]
           [java.io RandomAccessFile]))

(defrecord drumEntry [key value aux check update order result])
(defn drum-entry
  [{:keys [key value aux check update order result]}]
  (->drumEntry key value aux check update order result))

(defn size-in-bytes
  "Returns the number of bytes used to represent x in memory."
  [x]
  (measure x :bytes true))

(def MD (MessageDigest/getInstance "SHA-256"))
(defn eight-byte-hash
  "Returns an eight byte hash of the input string in hex-string format."
  [input]
  (let [bytes (.digest MD (.getBytes input))
        shorter-bytes (Arrays/copyOfRange bytes 0 8)]
    (DatatypeConverter/printHexBinary shorter-bytes)))

(defn hash->bytes
  "Returns byte array of the hex-string."
  [hex]
  (DatatypeConverter/parseHexBinary hex))

(defn file-size
  "Returns the file size in bytes."
  [full-path]
  (.length (clojure.java.io/file full-path)))

(defn set-file-size
  "Uses RandomAccessFile to set the file size."
  [filename size]
  (let [raf (RandomAccessFile. (io/file filename) "rwd")]
    (.setLength raf size)
    (.close raf)))

