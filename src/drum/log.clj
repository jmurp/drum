(ns drum.log
  (:import [java.util Date]))

(defn log-insert
  ([name drum]
   (spit ("./drum_logs/" name ".insert.log")
         (str "INSERT :: " (.toString (Date.)) " :: TRY LOCK FAILED, WROTE TO OVERFLOW\n"
              "\tDRUM :: " drum "\n")
         :append true))
  ([name sent-merge drum]
   (spit (str "./drum_logs/" name ".insert.log")
         (str "INSERT :: " (.toString (Date.)) " :: LOCK SUCCESS\n"
              "\tSENT MERGE_ACTION -> " sent-merge "\n"
              "\tDRUM :: " drum "\n")
         :append true)))

(defn log-merge
  ([name]
   (spit (str "./drum_logs/" name ".merge.log")
         (str "BEGIN MERGE :: " (.toString (Date.)) "\n")
         :append true))
  ([name index]
   (spit (str "./drum_logs/" name ".merge.log")
         (str "\tMERGING BUCKET " index " :: " (.toString (Date.)) "\n")
         :append true))
  ([name _ lock-logic]
   (spit (str "./drum_logs/" name "merge.log")
         (if (boolean lock-logic)
           (str "\tLOCK ACQUIRED :: " (.toString (Date.)) "\n")
           (str "\tLOCK RELEASED :: " (.toString (Date.)) "\n"))
         :append true))
  ([name _ _ _]
   (spit (str "./drum_logs/" name "merge.log")
         (str "MERGE COMPLETE :: " (.toString (Date.)) "\n\n")
         :append true)))