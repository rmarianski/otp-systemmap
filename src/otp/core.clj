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
  "filter out nil values after running map"
  [f & colls]
  (filter #(not (nil? %)) (apply map f colls)))

(defn duplicates
  "find the duplicates in a collection"
  [coll]
  (loop [coll coll acc #{} dupes []]
    (if (empty? coll)
      dupes
      (let [val (first coll)
            xs (rest coll)]
        (if (contains? acc val)
          (recur xs acc (conj dupes val))
          (recur xs (conj acc val) dupes))))))

(defn smallest-and-largest
  "retrieve the smallest and largest values from a seq"
  [key-fn coll]
  (let [sorted-seq (sort-by key-fn coll)]
    (vector (first sorted-seq) (last sorted-seq))))

(defmacro defreduce->map
  "concise forms to create a reduce that returns a hashmap"
  [[map-var curelt-var mapkey-var] keyform valform itemsform]
  `(reduce (fn [~map-var ~curelt-var]
             (let [~mapkey-var ~keyform]
              (assoc ~map-var ~mapkey-var ~valform)))
           {}
           ~itemsform))

(defmacro defreduce->map-conjoined
  "more specific instance of defreduce->macro to return a hashmap of conjoined values"
  [[curelt-var mapkey-var] keyform valform itemsform & default-value]
  `(defreduce->map
     [map-var# ~curelt-var ~mapkey-var]
     ~keyform
     (conj (get map-var#
                ~mapkey-var
                ~(if (nil? default-value) [] (first default-value)))
           ~valform)
     ~itemsform))

(def comma-sep (partial str-join ","))

;; general gtfs functions

(defn default-agency-id [] (:default-agency-id (read-config-file)))

(defn read-gtfs
  "return a loaded gtfscontext from the path"
  ([] (read-gtfs (:gtfs-file-path (read-config-file))))
  ([path] (read-gtfs path (default-agency-id)))
  ([path default-agency-id]
     (println "Reading gtfs file:" (-> path File. .getPath))
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

(defn has-shape?
  "returns whether a particular trip refers to a shape"
  [trip]
  (not-empty (.. trip getShapeId getId)))

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

;; mapping creation

(defn make-stopid-to-stoptimes
  "create a mapping of stopids to stoptimes"
  [dao]
  (defreduce->map-conjoined [stoptime stopid]
    (.. stoptime getStop getId)
    stoptime
    (.getAllStopTimes dao)))

(defn make-routeid-to-stopids
  "create a mapping of routeid to stop ids"
  [dao]
  (defreduce->map-conjoined [stoptime routeid]
    (.. stoptime getTrip getRoute getId)
    (.. stoptime getStop getId)
    (.getAllStopTimes dao)
    #{}))

(defn make-stopid-to-routeids [dao]
  "make a mapping of stopid -> route ids"
  (defreduce->map-conjoined [stoptime stopid]
    (.. stoptime getStop getId)
    (.. stoptime getTrip getRoute getId)
    (.getAllStopTimes dao)
    #{}))

(defn make-tripid-to-stoptimes [dao]
  "make a mapping of tripid -> stoptime objects"
  (defreduce->map-conjoined [stoptime tripid]
    (.. stoptime getTrip getId)
    stoptime
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
                                       departureTime (make-date-from-gtfs-time
                                                      (.getDepartureTime stoptime))]
                                   (if (and (contains? active-service-ids
                                                       (.getServiceId trip))
                                            (pos? (.compareTo departureTime date)))
                                     {:date departureTime
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
       (web-wms (create-gtfs-mappings) (geoserver-base-uri) params))
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
