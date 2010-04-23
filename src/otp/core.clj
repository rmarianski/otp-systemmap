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

;; this should be in a user.clj file
(defn meths [inst] (map #(.getName %) (.getMethods (class inst))))

(defn read-gtfs
  "return a loaded gtfscontext from the path"
  ; need to update representative trip ids first
  ;([] (read-gtfs "/home/rob/data/otp/mta/nyct_subway_100308.zip"))
  ([] (read-gtfs "/home/rob/data/otp/mta/theonetrain.zip"))
  ([path] (read-gtfs path "MTA NYCT"))
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
  [id]
  ;; TODO hardcoded agency id
  (AgencyAndId. "MTA NYCT" id))

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

(defn make-latlon
  "generate a string representation of a latlon"
  ([stop] (make-latlon (.getLat stop) (.getLon stop)))
  ([lat lon] (str lat "," lon)))

(defn make-latlons-to-stopids
  "create a mapping of latlons to a set of stopids"
  [dao]
  (reduce (fn [map stop]
            (let [latlon (make-latlon stop)]
              (assoc map latlon
                     (conj (get map latlon #{}) (.getId stop)))))
          {}
          (.getAllStops dao)))

(defn make-stopid-to-stoptimes
  "create a mapping of stopids to stoptimes"
  [dao]
  (reduce (fn [map stoptime]
            (let [stopid (.getId (.getStop stoptime))]
              (assoc map stopid
                     (conj (get map stopid []) stoptime))))
          {}
          (.getAllStopTimes dao)))

(defn make-routeid-to-shapepoints
  "create a mapping of routeid to shapepoints"
  [dao]
  (reduce (fn [map trip]
            (assoc map
              (.getId trip)
              (.getShapePointsForShapeId dao
                                         (.getShapeId trip))))
          {}
          (filter representative-trip? (.getAllTrips dao))))

(defn make-routeid-to-stopids
  "create a mapping of routeid to stop ids"
  [dao]
  (reduce (fn [map stoptime]
            (let [stop (.getStop stoptime)
                  route (.getRoute (.getTrip stoptime))
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
                  route (.getRoute (.getTrip stoptime))
                  stopid (.getId stop)
                  routeid (.getId route)]
              (assoc map
                stopid (conj (get map stopid #{})
                             routeid))))
          {}
          (filter #(representative-trip? (.getTrip %))
                  (.getAllStopTimes dao))))

(defn create-gtfs-mappings [& filename]
  (let [daomap (if filename
                 (read-gtfs (first filename))
                 (read-gtfs))
        dao (:dao daomap)
        calendar (:calendar daomap)]
    {:latlons-to-stopids (make-latlons-to-stopids dao)
     :stopid-to-stoptimes (make-stopid-to-stoptimes dao)
     :stopid-to-routeids (make-stopid-to-routeids dao)
     :routeid-to-shapepoints (make-routeid-to-shapepoints dao)
     :routeid-to-stopids (make-routeid-to-stopids dao)
     :dao dao
     :calendar calendar}))

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

;; should update the parameters for this function
(defn get-departures-for-stops
  "retrieve departure info for given stops"
  ([gtfs-mapping stops]
     (get-departures-for-stops gtfs-mapping stops (Date.)))
  ([{:keys [calendar stopid-to-stoptimes]} stops date]
     (let [stoptimes (mapcat #(stopid-to-stoptimes (.getId %)) stops)
           active-service-ids (set (.getServiceIdsOnDate calendar (ServiceDate. date)))]
       (filter
        (complement nil?)
        (map (fn [stoptime]
               (let [trip (.getTrip stoptime)
                     departureTime (make-date-from-gtfs-time (.getDepartureTime stoptime))]
                 (if (and (contains? active-service-ids
                                     (.getServiceId trip))
                          (pos? (.compareTo departureTime date)))
                   {:date departureTime
                    :headsign (.getTripHeadsign trip)
                    :route (make-detailed-route (.getRoute trip))
                    :routeid (.getId (.getRoute trip))})))
             stoptimes)))))

(defn make-detailed-stop [{:keys [dao calendar
                                  stopid-to-stoptimes stopid-to-routeids] :as gtfs-mapping}
                          stop]
  {:name (.getName stop)
   :stopId (.. stop getId getId)
   :routes (map #(make-detailed-route %)
                (map
                 #(.getRouteForId dao %)
                 (get stopid-to-routeids
                      (.getId stop)
                      [])))
   :departures (take 3 (sort-by :date (get-departures-for-stops gtfs-mapping
                                                                [stop])))})

(defn parse-response
  "parse the geoserver response"
  [response-string]
  (filter
   #(and (not (empty? %))
         (let [x (first %)]
           (or (= x "routes")
               (= x "stops"))))
   (let [responses (filter
                    (complement empty?)
                    (re-split #"\s" response-string))]
     (map #(re-split #"\." % 2) responses))))

(defn web-wms [{:keys [dao routeid-to-stopids] :as gtfs-mapping} geoserver-base-uri params]
  (json-str
   (or 
    (let [request-uri (url-params geoserver-base-uri params)
          geoserver-response (.trim (slurp* request-uri))
          parsed-responses (parse-response geoserver-response)]
      (if (not (empty? parsed-responses))
        (if-let [stopid (some #(if (= "stops" (first %)) (second %)) parsed-responses)]
          (if-let [stop (.getStopForId dao (make-id stopid))]
            {:type :stop
             :stop (make-detailed-stop gtfs-mapping stop)})
          (let [routeids (map #(make-id (second %)) parsed-responses)
                routes (filter (complement nil?)
                               (map #(.getRouteForId dao %) routeids))]
            {:type :routes
             :routes (map make-detailed-route routes)
             :stopids (mapcat (fn [routeid]
                                (map #(.getId %)
                                     (get routeid-to-stopids routeid [])))
                              routeids)})))
      )
    {})))

;; populate the gtfs mappings when a request comes in the first time
(defonce *gtfs-mapping* (atom nil))

; macro that uses a create-gtfs-mappings function
; that only loads the data once and stores it in an atom
; for some reason the memoize function wasn't caching enough
; also adds local bindings read from the config file whose path
; is specified above *cfg-file*
(defmacro defgtfsroutes [name & routes]
  `(let [cfg# (with-in-reader *cfg-file* (read))
         ;~'create-gtfs-mappings (memoize create-gtfs-mappings)
         ;; atom might be a better approach
         ~'create-gtfs-mappings (fn []
                                  (if @*gtfs-mapping*
                                    @*gtfs-mapping*
                                    (swap! *gtfs-mapping* create-gtfs-mappings)))
         ~'geoserver-base-uri (:geoserver-base-uri cfg#)
         ~'url-prefix (:url-prefix cfg#)
         ~'base-path (:base-path cfg#)]
     (defroutes ~name ~@routes)))

(defgtfsroutes weburls
  (GET (str url-prefix "/") (html [:body [:h1 "hi world"]]))
  (GET (str url-prefix "/wms")
       (web-wms (create-gtfs-mappings) geoserver-base-uri params)))
  ;; this is just for testing locally
;;   (GET (str url-prefix "/*")
;;        (or (serve-file (str base-path "/api-extended") (params :*)) :next))
;;   (ANY "/*"
;;        (page-not-found (str base-path "/public/404.html"))))

(defn -main [& args]
  (let [port (if args (Integer. (first args)) 2468)]
    (run-server {:port port}
                "/*" (->> weburls servlet))))

(defservice weburls)
