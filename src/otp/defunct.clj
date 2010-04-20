;; need to sort out the lein dependency issue with the extended api
;; before we can use this function
;; (defn make-transit-gtfs
;;   "make the otp assembled api extended transit server gtfs"
;;   ([] (make-transit-gtfs "/home/rob/data/otp/mta/theonetrain.zip"))
;;   ([path]
;;      (doto (TransitServerGtfs.)
;;        (.setGtfsFile (File. path))
;;        (.initialize))))

