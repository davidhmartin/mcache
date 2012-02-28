(ns mcache.util
  "Helper functions considered to be private to the mcache namespaces")

(defn cache-update-multi
  "Used by add, set, and replace functions which operate on map of key/value pairs.
   Calls the updating function iteratively over each key/val pair, and returns a
   map of key to Future<Boolean>, where the boolean indicates whether the val
   associated with the key was added."
  [cache-updating-fctn mc key-val-map exp]
  (reduce #(assoc %1 (first %2) (second %2)) {}
          (map (fn [k_v] [(first k_v) (cache-updating-fctn mc (first k_v) (second k_v) exp)]) key-val-map)))

