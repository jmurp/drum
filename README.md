# drum

this readme is out of date, will be updated soon

A Clojure implementation of DRUM (Disk Repository with Update Management).  The specifics of the DRUM design will not be discussed here.  See this group's work for more detail: [IRLbot](http://irl.cs.tamu.edu/crawler/).  Oracle Berkeley DB Java Edition is used for storage of <key,value> pairs.  This will be implemented in a web crawler under development.  **Send bug reports and suggestions to jmurphy@protonmail.com.**

## Usage

A drum can be created via 
```
(require '[drum.core :refer [create-drum]])

(create-drum 
   directory 
   drum-name
   num-buckets
   max-buf-size
   max-file-size
   overwrite-fn check-fn dispatch-fn)
```

The `overwrite-fn`, `check-fn`, and `dispatch-fn` must be written by you.

The `overwrite-fn` is called when determining how to overwrite existing data upon update operations.  It must take two string arguments \[^String old-data, ^String new-data\] and return the string representing what should be written to the database.

(this might be changed soon)
The `check-fn` is called to generate processed information to utilize in the `dispatch-fn`.  On check operations, it is called with one string argument \[^String data\] which is the current value on disk of the provided key if it existed.  If the key is not present in the database, the function is called with no arguments.  The return value of `check-fn` is appended to the entry map (continue reading for more) under an `:r` keyword.  This may be extracted and processed in the body of the `dispatch-fn`.

(this will be changed soon)
The `dispatch-fn` is called on the merge results of check operations.  Its argument is a map which looks like `{:key :value :aux :c :u :o :r}`.  The relevant keywords for processing are `:key :value :aux :r`.  This function is meant to be used to process the results.

Below are example implementations of the above functions.

```
(defn overwrite-fn
  "Overwrite the existing data with the new data."
  [old new]
  new)
  
(defn check-fn
  "Classify the key as unique or duplicate."
  ([] "unique")
  ([data] (str "duplicate::" data)))
  
(defn dispatch-fn
  "Print the results to *out*."
  [entry]
  (println entry))
```

Finally, drum may be used by inserting entries.  Entries are maps that look like `{:key :value :aux :c :u}`.  The `:key` value should be a string used to generate an 8-byte hash for identification in the database.  The `:value` value is the data to be written or processed in `overwrite-fn` when the `:key` is a duplicate.  The `:aux` value is additional information processed in `dispatch-fn` upon check operations.  Set `:c` to true to perform a check operation.  Set `:u` to true to perform an update operation.  Entries are added to drum via the `insert` function which takes the drum object returned by `create-drum` as its first argument and the entry as the second argument.
```
(require '[drum.core :refer [insert]])

(def check-entry {:key "identifier1" :aux "additional1" :c true})
(def update-entry {:key "identifier2" :value "data2" :u true})
(def check-and-update-entry {:key "identifier3" :value "data3" :aux "additional3" :c true :u true})

(insert drum-object check-entry)
(insert drum-object update-entry)
(insert drum-object check-and-update-entry)
```

The insert is handled by an agent so that the thread making the above call will never block.  This implementation is thread safe.  I recommend creating a partially called function with the created drum object and never touching it again.
```
(def insert-entry (partial insert drum))

...

(insert-entry entry)
```

The code does not yet handle errors or protect you from making tiny or catastrophic mistakes.

## License

Copyright © 2018 FIXME

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
