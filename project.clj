(defproject drum "0.1.0-SNAPSHOT"
  :description "Implementation of DRUM (disk repository with update management,
  see this group's work http://irl.cs.tamu.edu/crawler/) using Berkeley DB."
  :url "https://github.com/jmurp/drum"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [com.clojure-goes-fast/clj-memory-meter "0.1.0"]
                 [com.sleepycat/je "18.3.1"]]
  :java-source-paths ["src/"])
