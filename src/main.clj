(ns main
  (:require [clj-http.client :as client]
            [jsonista.core :as j]
            [clojure.java.io :as io]
            [clojure.data.xml :as xml]))

(comment
  (def b1 {:name "Nirvana" :genres ["grunge" "punk rock"]}))

#_(j/write-value-as-string b1)

(comment
  (let [{:keys [status body]} (client/get "http://localhost:9200/_aliases")]
    (if (= status 200)
      (j/read-value body j/keyword-keys-object-mapper))))

(comment
  (try
    (client/post "http://localhost:9200/bands/_doc"
      {:body (j/write-value-as-string b1)
       :content-type :json})
    (catch Exception e
      )))

;; es utils

(defn es-get [es-url doc id]
  (let [url (str es-url "/" doc "/_doc/" id)
        {:keys [body status]} (client/get url)]
   (if (= status 200)
     (j/read-value body j/keyword-keys-object-mapper))))

(defn es-post [es-url index doc]
  (let [url (str es-url "/" index "/_doc")
        {:keys [body status]} (client/post url
                                           {:body (j/write-value-as-string doc)
                                            :content-type :json})]
    (if (= status 200)
      (j/read-value body j/keyword-keys-object-mapper))))

(defn bulk [es-url index data]
  (let [url (str es-url "/" index "/_bulk")
        {:keys [body status]} (client/put url
                                {:body data
                                 :content-type :json})]
    (if (= status 200)
      (j/read-value body j/keyword-keys-object-mapper))))


(comment
  (es-get "http://localhost:9200" "bands" "RoPKO3kB5hKgiQVjkSeF"))


;; xml

(defn lower-keyword [k]
  (when k
    (-> k name clojure.string/lower-case keyword)))

#_(def band-xml (xml/parse-str (slurp (io/file "resources/temp/sample.xml"))))

(def doc-xml (xml/parse-str (slurp "/home/kostas/Downloads/MiniCollectionVirtual/1/EP-0639190.txt")))

(def doc-xml-2 (xml/parse-str (slurp "/home/kostas/Downloads/MiniCollectionVirtual/1/EP-0001200.txt")))

;; ugly dirty xml parsing

(declare xml->ds)

(defn parse-tag [content]
  (if (> (count content) 1)
    (let [d (mapv xml->ds content)]
      (if (= (:tag (first content)) (:tag (second content)))
        d
        (apply merge d)))
    (if (string? (first content))
      (first content)
      (xml->ds (first content)))))

(defn xml->ds [xml-data]
  (when xml-data
    (let [{:keys [tag content]} xml-data]
      (assoc {} (lower-keyword tag)
                (parse-tag content)))))

(comment
  (es-post "http://localhost:9200" "bands" (:doc (xml->ds band-xml))))

(comment
  (spit "resources/sample.json" (j/write-value-as-string (xml->ds doc-xml-2))))

(comment
  (xml->ds doc-xml))

;; prepare collection for index

(defn file? [^java.io.File f]
  (.isFile f))

(defn check-docs [dir]
  (let [files (filter file? (file-seq (io/file dir)))]
    (doseq [f files]
      (try
        (xml->ds (xml/parse-str (slurp f)))
        (catch Exception e
          (println "Cannot process file " f))))))

(comment
  (check-docs "/home/kostas/Downloads/MiniCollectionVirtual"))

(defn prepare-docs [dir out-file]
  ;; clear existing content
  (spit out-file "")
  (let [files (filter file? (file-seq (io/file dir)))]
    (doseq [f files]
      (println f)
      (try
        (spit out-file "{ \"create\": { } }\n" :append true)
        (spit out-file (j/write-value-as-string (:doc (xml->ds (xml/parse-str (slurp f))))) :append true)
        (spit out-file "\n" :append true)
        (catch Exception e
          (print "."))))))

(comment
  (prepare-docs "/home/kostas/Downloads/MiniCollectionVirtual" "resources/temp/bulk.json"))

;; insert docs

;; to big ... fails
(comment
  (bulk "http://localhost:9200" "patents"
    (slurp "resources/temp/bulk.json")))

(defn insert-docs [dir]
  ;; clear existing content
  (let [files (filter file? (file-seq (io/file dir)))]
    (doseq [f files]
      (try
        (let [doc (:doc (xml->ds (xml/parse-str (slurp f))))]
          (es-post "http://localhost:9200" "patents" doc))
        (print ".")
        (catch Exception e)))))

(comment
  (insert-docs "/home/kostas/Downloads/MiniCollectionVirtual"))
