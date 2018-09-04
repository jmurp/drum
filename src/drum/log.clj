(ns drum.log
  (:import [java.util Date]))

(defn log-insert
  ([name drum]
   (spit ("./drum_logs/" name ".log")
         (str "INSERT :: " (.toString (Date.)) " :: TRY LOCK FAILED, WROTE TO OVERFLOW\n"
              "DRUM :: " drum "\n")
         :append true))
  ([name sent-merge drum]
   (spit (str "./drum_logs/" name ".log")
         (str "INSERT :: " (.toString (Date.)) " :: LOCK SUCCESS\n"
              "SENT MERGE_ACTION -> " sent-merge "\n"
              "DRUM :: " drum "\n")
         :append true)))
