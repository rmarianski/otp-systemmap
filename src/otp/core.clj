(ns otp.core
  (:gen-class :extends javax.servlet.http.HttpServlet)
  (:use [compojure]
        [compojure.http response]
        [clojure.contrib.lazy-xml :only (emit)]
        [clojure.contrib.json.write :only (json-str print-json)]
        [clojure.contrib.str-utils :only (str-join re-split)]
        [clojure.contrib.duck-streams :only (slurp* with-in-reader)])
  (:import (java.io File)
           (java.util Calendar Date)
           (org.onebusaway.gtfs.model AgencyAndId)
           (org.onebusaway.gtfs.model.calendar ServiceDate)
           (org.onebusaway.gtfs.serialization GtfsReader)
           (org.onebusaway.gtfs.impl GtfsRelationalDaoImpl)
           (org.onebusaway.gtfs.impl.calendar CalendarServiceDataFactoryImpl CalendarServiceImpl)))

(def *cfg-file* "/home/rob/dev/java/clojure/otp/src/otp/config.clj")

(defn read-config-file [] (with-in-reader *cfg-file* (read)))

;; this should be in a user.clj file
(defn meths [inst] (map #(.getName %) (.getMethods (class inst))))

;; utilities?
(defn filter-map
  "filter out nil values after running map on one collection"
  [f coll]
  (filter #(not (nil? %)) (map f coll)))

(defn default-agency-id [] (:default-agency-id (read-config-file)))

(defn read-gtfs
  "return a loaded gtfscontext from the path"
  ; need to update representative trip ids first
  ;([] (read-gtfs "/home/rob/data/otp/mta/nyct_subway_100308.zip"))
  ([] (read-gtfs (:gtfs-file-path (read-config-file))))
  ([path] (read-gtfs path (default-agency-id)))
  ([path default-agency-id]
     (let [dao (GtfsRelationalDaoImpl.)
           gtfs-reader
           (doto (GtfsReader.)
             (.setInputLocation (File. path))
             (.setEntityStore dao)
             (.setDefaultAgencyId default-agency-id))]
       (.run gtfs-reader)
       (let [factory (doto (CalendarServiceDataFactoryImpl.)
                       (.setGtfsDao dao))
             calendar (doto (CalendarServiceImpl.)
                        (.setData (.createData factory)))]
         {:dao dao :calendar calendar}))))

(defn make-id
  "create an AgencyAndId object for a given string id"
  ([id] (make-id (default-agency-id) id))
  ([agency-id id] (AgencyAndId. agency-id id)))

(defn make-date-from-gtfs-time [time]
  "create a java.util.Date instance from a gtfs time integer"
  (.getTime
   (doto (Calendar/getInstance)
     (.setTimeInMillis (System/currentTimeMillis))
     (.set Calendar/HOUR_OF_DAY 0)
     (.set Calendar/MINUTE 0)
     (.set Calendar/SECOND 0)
     (.set Calendar/MILLISECOND 0)
     (.add Calendar/SECOND time))))

;; trip ids that are representative for nyc
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

(defn smallest-and-largest
  "retrieve the smallest and largest values from a seq"
  [coll key-fn]
  (let [sorted-seq (sort-by key-fn coll)]
    (vector (first sorted-seq) (last sorted-seq))))

(defn make-trip-stop-struct
  "convert a map entry of tripid to stoptimes to a trip stop struct"
  [tripid-stoptimes-entry]
  (let [tripid (key tripid-stoptimes-entry)
        stoptimes (val tripid-stoptimes-entry)
        first-last-stoptime (smallest-and-largest stoptimes #(.getDepartureTime %))
        first-last-stop (map #(.. % getStop getId) first-last-stoptime)]
    {:tripid tripid
     :first-stop (first first-last-stop)
     :last-stop (second first-last-stop)}))

(defn has-shape?
  "returns whether a particular trip refers to a shape"
  [trip]
  (not-empty (.. trip getShapeId getId)))

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
        unique-tripids (reduce (fn [map {:keys [first-stop last-stop tripid]}]
                                 (assoc map [first-stop last-stop] tripid))
                               {}
                               representative-trip-stop-structs)]
    (vals unique-tripids)))

(defn make-representativetrip-to-stops
  "return a mapping of the representative trips to their start/end stops"
  ([dao] (make-representativetrip-to-stops dao (representative-stoptimes dao)))
  ([dao rep-stoptimes]
     (let [trip->deptime-stops
           (reduce (fn [map stoptime]
                     (let [tripid (.. stoptime getTrip getId)]
                       (assoc map tripid
                              (conj (get map tripid #{})
                                    {:deptime (-> stoptime .getDepartureTime make-date-from-gtfs-time)
                                     :stop (.. stoptime getStop getId getId)}))))
                   {}
                   rep-stoptimes)]
       (reduce (fn [hashmap [tripid deptime-stops]]
                 (assoc hashmap
                   (.getId tripid) (map :stop (smallest-and-largest deptime-stops :deptime))))
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

(defn make-stopid-to-stoptimes
  "create a mapping of stopids to stoptimes"
  [dao]
  (reduce (fn [map stoptime]
            (let [stopid (.. stoptime getStop getId)]
              (assoc map stopid
                     (conj (get map stopid []) stoptime))))
          {}
          (.getAllStopTimes dao)))

(defn make-routeid-to-stopids
  "create a mapping of routeid to stop ids"
  [dao]
  (reduce (fn [map stoptime]
            (let [stop (.getStop stoptime)
                  route (.. stoptime getTrip getRoute)
                  stopid (.getId stop)
                  routeid (.getId route)]
              (assoc map
                routeid (conj (get map routeid #{})
                              stopid))))
          {}
          (filter #(representative-trip? (.getTrip %))
                  (.getAllStopTimes dao))))

(defn make-stopid-to-routeids [dao]
  "make a mapping of stopid -> route ids"
  (reduce (fn [map stoptime]
            (let [stop (.getStop stoptime)
                  route (.. stoptime getTrip getRoute)
                  stopid (.getId stop)
                  routeid (.getId route)]
              (assoc map
                stopid (conj (get map stopid #{})
                             routeid))))
          {}
          (filter #(representative-trip? (.getTrip %))
                  (.getAllStopTimes dao))))

; maybe we should map to stoptime ids instead
; to be leaner on mem usage?
(defn make-tripid-to-stoptimes [dao]
  "make a mapping of tripid -> stoptime objects"
  (reduce (fn [map stoptime]
            (let [tripid (.. stoptime getTrip getId)]
              (assoc map tripid
                     (conj (get map tripid [])
                           stoptime))))
          {}
          (.getAllStopTimes dao)))
                  
(defn create-gtfs-mappings [& filename]
  (let [daomap (if filename
                 (read-gtfs (first filename))
                 (read-gtfs))
        dao (:dao daomap)]
    {:stopid-to-stoptimes (make-stopid-to-stoptimes dao)
     :stopid-to-routeids (make-stopid-to-routeids dao)
     :routeid-to-stopids (make-routeid-to-stopids dao)
     :tripid-to-stoptimes (make-tripid-to-stoptimes dao)
     :dao dao
     :calendar (:calendar daomap)}))

;; json formatting multimethod definitions
(defmethod print-json java.util.Date [x]
  (print-json (.format (java.text.SimpleDateFormat. "yyyy-MM-dd'T'HH:mm:ss") x)))

(defmethod print-json AgencyAndId [x]
  (print-json (str x)))

(defn route-type-to-string
  "convert the route integer type to a string"
  [type]
  (condp = type
    0 "TRAM"
    1 "SUBWAY"
    2 "RAIL"
    3 "BUS"
    4 "FERRY"
    5 "CABLE_CAR"
    6 "GONDOLA"
    7 "FUNICULAR"))

;; consider creating automatic json serializers
(defn make-detailed-route [route]
  (let [agency-and-id (.getId route)]
    {:shortName (.getShortName route)
     :longName (.getLongName route)
     :mode (route-type-to-string (.getType route))
     :agencyId (.getAgencyId agency-and-id)
     :routeId (.getId agency-and-id)}))

(defn get-departures-for-stops
  "retrieve departure info for given stops"
  ([gtfs-mapping stops]
     (get-departures-for-stops gtfs-mapping stops (Date.)))
  ([{:keys [calendar stopid-to-stoptimes]} stops date]
     (let [stoptimes (mapcat #(stopid-to-stoptimes (.getId %)) stops)
           active-service-ids (set (.getServiceIdsOnDate calendar (ServiceDate. date)))
           make-departure-info (fn [stoptime]
                                 (let [trip (.getTrip stoptime)
                                       date (make-date-from-gtfs-time
                                             (.getDepartureTime stoptime))]
                                   (if (and (contains? active-service-ids
                                                       (.getServiceId trip))
                                            (pos? (.compareTo date date)))
                                     {:date date
                                      :headsign (.getTripHeadsign trip)
                                      :route (make-detailed-route (.getRoute trip))
                                      :routeid (.. trip getRoute getId)})))]
       (filter-map make-departure-info stoptimes))))

(defn make-detailed-stop
  ([gtfs-mapping stop] (make-detailed-stop gtfs-mapping stop 3))
  ([{:keys [dao stopid-to-routeids] :as gtfs-mapping} stop n]
     (let [routeids (get stopid-to-routeids (.getId stop) [])
           routes (map #(.getRouteForId dao %) routeids)
           departures (get-departures-for-stops gtfs-mapping [stop])]
       {:name (.getName stop)
        :stopId (.. stop getId getId)
        :routes (map #(make-detailed-route %) routes)
        :departures (take n (sort-by :date departures))})))

(defn parse-geoserver-response [response-string]
  (loop [m (re-matcher #"(stops|routes).(\S+)" response-string)
         acc []]
    (if (re-find m)
      (recur m (conj acc (rest (re-groups m))))
      acc)))

(defn web-wms [{:keys [dao routeid-to-stopids] :as gtfs-mapping} geoserver-base-uri params]
  (json-str
   (or 
    (let [request-uri (url-params geoserver-base-uri params)
          geoserver-response (-> request-uri slurp* .trim)
          parsed-responses (parse-geoserver-response geoserver-response)]
      (if (not-empty parsed-responses)
        (if-let [stopid (some #(if (= "stops" (first %)) (second %)) parsed-responses)]
          (if-let [stop (.getStopForId dao (make-id stopid))]
            {:type :stop
             :stop (make-detailed-stop gtfs-mapping stop)})
          (let [routeids (map #(make-id (second %)) parsed-responses)
                routes (filter-map #(.getRouteForId dao %) routeids)
                make-route->stopids (fn [routeid]
                                      (map #(.getId %)
                                           (get routeid-to-stopids routeid [])))]
            {:type :routes
             :routes (map make-detailed-route routes)
             :stopids (mapcat make-route->stopids routeids)}))))
    {})))

;;; consider creating a macro that abstracts over the ref nil
;;; and function to set it first pattern
;;; as well as function to retrieve the value, and set it first if necessary

;; populate the gtfs mappings when a request comes in the first time
(defonce *gtfs-mapping* (ref nil))

(defn set-gtfs-mappings []
  (dosync (ref-set *gtfs-mapping* (create-gtfs-mappings))))

; macro that uses a create-gtfs-mappings function
; that only loads the data once and stores it in an atom
; for some reason the memoize function wasn't caching enough
; maybe the servlet was being removed from memory when not in use?
; it also injects functions for all keys in in the config file
; that read the config file on each call and return the corresponding value
(defmacro defgtfsroutes [routesname & routes]
  (let [cfgkeys (keys (read-config-file))]
    `(let [~'create-gtfs-mappings (fn []
                                    (if @*gtfs-mapping*
                                      @*gtfs-mapping*
                                      (set-gtfs-mappings)))
           ~@(mapcat (fn [cfgkey]
                       `(~(symbol (name cfgkey))
                         (fn [] (get (read-config-file) ~cfgkey))))
                     cfgkeys)]
       (defroutes ~routesname ~@routes))))

(defgtfsroutes weburls
  (GET (str (url-prefix) "/") (html [:body [:h1 "hi world"]]))
  (GET (str (url-prefix) "/wms")
       (web-wms (create-gtfs-mappings) (geoserver-base-uri) params)))
  ;; this is just for testing locally
;;   (GET (str (url-prefix) "/*")
;;        (or (serve-file (str (base-path) "/api-extended") (params :*)) :next))
;;   (ANY "/*"
;;        (page-not-found (str (base-path) "/public/404.html"))))

(defn -main [& args]
  (let [port (if args (Integer. (first args)) 2468)]
    (run-server {:port port}
                "/*" (->> weburls servlet))))

(defservice weburls)
