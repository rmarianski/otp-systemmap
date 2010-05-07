;;;; this module contains the code surrounding representative trips

(ns otp.reptrips
  (:use [otp.core]
        [clojure.contrib.str-utils :only (str-join re-split)]
        [compojure :only (html)]))

;; trip ids that are representative for nyc
;; using theonetrain.zip gtfs data
(def representative-tripids
     (set (map make-id
               ["A20100125W_048000_1..S03R"
                "A20100125W_048050_2..S01R"
                "A20100125W_048350_3..S01R"
                "A20100125A_048450_4..S06R"
                "A20100125W_053850_5..S03R"
                "A20100125W_048000_6..S02R"
                "A20100125U_048300_6..N01R"
                "A20100125W_048200_7..S14R"
                "A20100125A_048300_7..S01R"
                "B20100125W_048450_A..S55R"
                "B20100125W_048250_B..S45R"
                "B20100125A_048300_C..S04R"
                "B20100125W_048300_D..S14R"
                "B20100125W_049100_E..S56R"
                "B20100125U_048250_F..S69R"
                "B20100125U_048050_G..N12R"
                "B20100125W_048000_J..N12R"
                "B20100125A_048400_L..S01R"
                "B20100125W_048550_M..N71R"
                "B20100125W_048700_N..S34R"
                "B20100125A_048000_Q..S41R"
                "B20100125W_048350_R..N93R"
                "A20100125A_048000_GS.S01R"
                "B20100125W_048150_FS.S01R"
                "B20100125U_048100_H..S21R"
                "S20100125A_048100_SI.S01R"
                "B20100125W_048150_V..S01R"
                "B20100125W_048300_W..S25R"
                "B20100125W_101500_J..N16R"])))

(defn representative-trip?
  "predicate whether given trip is representative of its route"
  [trip]
  (contains? representative-tripids (.getId trip)))

(defn representative-stoptimes
  "return the stoptimes that refer to a representative trip"
  [dao]
  (filter #(representative-trip? (.getTrip %)) (.getAllStopTimes dao)))

(defn make-trip-stop-struct
  "convert a map entry of tripid to stoptimes to a trip stop struct"
  [tripid-stoptimes-entry]
  (let [tripid (key tripid-stoptimes-entry)
        stoptimes (val tripid-stoptimes-entry)
        first-last-stoptime (smallest-and-largest #(.getDepartureTime %) stoptimes)
        first-last-stop (map #(.. % getStop getId) first-last-stoptime)]
    {:tripid tripid
     :first-stop (first first-last-stop)
     :last-stop (second first-last-stop)}))

(defn reptrips-for-stopids
  "retrieve the trips that have the start/end stopids (stopids are agencyandids)"
  [stopid-pairs {:keys [dao tripid-to-stoptimes]}]
  (let [get-trip (fn [tid->st] (->> tid->st key (.getTripForId dao)))
        trip-stop-structs (for [tid->st tripid-to-stoptimes
                                :when (has-shape? (get-trip tid->st))]
                            (make-trip-stop-struct tid->st))
        stops-match? (fn [{:keys [first-stop last-stop]}]
                       (some (fn [[pair-first-stop pair-last-stop]]
                               (and (= pair-first-stop first-stop)
                                    (= pair-last-stop last-stop)))
                             stopid-pairs))
        representative-trip-stop-structs (filter stops-match? trip-stop-structs)                       
        unique-tripids (reduce (fn [acc {:keys [first-stop last-stop tripid]}]
                                 (assoc acc [first-stop last-stop] tripid))
                               {}
                               representative-trip-stop-structs)]
    (vals unique-tripids)))

(defn make-representativetrip-to-stops
  "return a mapping of the representative trips to their start/end stops"
  ([dao] (make-representativetrip-to-stops dao (representative-stoptimes dao)))
  ([dao rep-stoptimes]
     (let [trip->deptime-stops
           (reduce (fn [acc stoptime]
                     (let [tripid (.. stoptime getTrip getId)]
                       (assoc acc tripid
                              (conj (get acc tripid #{})
                                    {:deptime (-> stoptime .getDepartureTime make-date-from-gtfs-time)
                                     :stop (.. stoptime getStop getId getId)}))))
                   {}
                   rep-stoptimes)]
       (reduce (fn [acc [tripid deptime-stops]]
                 (assoc acc
                   (.getId tripid) (map :stop (smallest-and-largest :deptime deptime-stops))))
               {}
               trip->deptime-stops))))

(defn make-stopid-pairs
  "convenience function to create stopid pairs suitable for reptrips-for-stopids fn from representative trips"
  [dao]
  (map #(map make-id %) (vals (make-representativetrip-to-stops dao))))

(defn make-trip-route-shapeid-struct
  "create a more useful structure for generating shape map from trip ids (agencyandids)"
  [dao tripids]
  (map #(let [trip (.getTripForId dao %)]
              (list (.getId %)
                    (.. trip getRoute getId getId)
                    (.. trip getShapeId getId)))
       tripids))

(defn make-route-shapeid-str [route-shapeid-structs]
  (map #(str-join "," %) (map rest route-shapeid-structs)))

(defn get-trips-for-shapeids [dao shapeids]
  (let [shapeids (set shapeids)
        reptrips (filter #(shapeids (.getShapeId %))
                         (.getAllTrips dao))
        ; to uniquify
        shapeid->reptrips (reduce #(assoc % (.getShapeId %2) %2)
                                    {}
                                    reptrips)]
    (vals shapeid->reptrips)))

(defn web-shape-mapping [{:keys [dao] :as gtfs-mapping}]
  (html
   [:ul
    (let [stopid-pairs (make-stopid-pairs dao)
          tripids (reptrips-for-stopids stopid-pairs gtfs-mapping)
          route-shape-structs (make-trip-route-shapeid-struct dao tripids)
          route-shapeid-strs (make-route-shapeid-str route-shape-structs)]
      (map #(vector :li %) (sort route-shapeid-strs)))]))

(defn web-shape-mapping-with-shapeids [dao reptripids]
  (html
   [:ul
    (let [reptrips (map #(.getTripForId dao %) reptripids)
          route-shapes (map #(list (.. % getRoute getId getId)
                                   (.. % getShapeId getId))
                            reptrips)
          route-shapes (sort-by first route-shapes)]
      (map #(vector :li (comma-sep %)) route-shapes))]))

(defn web-route-stops [dao]
  (html
   [:ul
    (let [reptrips->stops (make-representativetrip-to-stops dao)
          route-stops-pair (map #(concat
                                  (list
                                   (.. (->> % key make-id (.getTripForId dao))
                                       getRoute getId getId))
                                  (val %))
                                  reptrips->stops)]
      (map #(vector :li (comma-sep %)) (sort-by first route-stops-pair)))]))

(comment
  (GET (str (url-prefix) "/trip-shape-mapping") (web-shape-mapping (create-gtfs-mappings)))
  (GET (str (url-prefix) "/simple-trip-shape-mapping")
       (web-shape-mapping-with-shapeids (:dao (create-gtfs-mappings)) representative-tripids))
  (GET (str (url-prefix) "/route-stops") (web-route-stops (:dao (create-gtfs-mappings))))
)
