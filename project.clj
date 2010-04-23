(defproject otp "1.0.0-SNAPSHOT"
  :description "some otp stuff"
  :dependencies [[org.clojure/clojure "1.1.0"]
                 [org.clojure/clojure-contrib "1.1.0"]
                 [org.slf4j/slf4j-simple "1.5.2"]
                 [org.slf4j/slf4j-api "1.5.2"]
                 [compojure "0.3.2"]
                 [swank-clojure "1.1.0"]
                 [commons-beanutils "1.8.3"]
                 ; otp stuff isn't necessary
                 ;[org.opentripplanner/opentripplanner-routing "0.3-SNAPSHOT"]
                 ;; the extended one gives me problems
                 ;; and it's because it's a war file and not a jar
                 ;[org.opentripplanner/opentripplanner-api-extended "0.3-SNAPSHOT"]
                 [org.onebusaway/onebusaway-gtfs "1.1.2"]]
  :dev-dependencies [[uk.org.alienscience/leiningen-war "0.0.2"]]
  :namespaces [otp.core])
