(require '[babashka.fs :as fs]
         '[cheshire.core :as json]
         '[babashka.http-client :as http])

(def SORTER_API_BASE_URL "http://localhost:8081/api")
(def SORTER_API_KEY "STATIC_TEST_KEY_FOR_ADMIN_USER_123")

(when (or (empty? SORTER_API_BASE_URL) (empty? SORTER_API_KEY))
  (println "ERROR: SORTER_API_URL and SORTER_API_KEY environment variables must be set.")
  (System/exit 1))


(defn post-to-api [endpoint data]
  (println "POST ->" endpoint)
  (let [payload (json/generate-string data)
        response (http/post (str SORTER_API_BASE_URL endpoint)
                            {:headers {"Content-Type" "application/json"
                                       "Authorization" (str "Bearer " SORTER_API_KEY)}
                             :query-params {"upsert" "true"}
                             :body payload})]
    (when (not= 200 (:status response))
      (println "HTTP" (:status response) (:body response)))))

(println "--- PASS 1: Ingesting Entities ---")
(let [entity-files (fs/glob "." "**/*.entity")]
  (doseq [file entity-files]
    (post-to-api "/entity"
                 (-> file fs/file slurp (json/parse-string true)))))

;; --- PASS 2: Ingest all relationships (Edges) ---
;; This creates the parent "collection" entities and wires up the children.
(println "\n--- PASS 2: Ingesting Relationships ---")
(let [relationship-files (fs/glob "." "**/*.relationships")]
  (doseq [rel-file relationship-files]
    (def rel-file rel-file)
    (let [relationships (-> rel-file fs/file slurp (json/parse-string true))]
      (doseq [rel relationships]
        ;; First, create the parent "collection" entity itself.
        (def rel rel)
        ;; Then, resolve the children via glob and create the links.
        (doseq [glob-pattern (:children_glob rel)]
          (def glob-pattern glob-pattern)
          (let [child-files (fs/glob (fs/parent rel-file) "*")]
            (println fs-glob)
            (println child-files)
            (doseq [child-file child-files]
              (let [child-entity (-> child-file slurp (json/parse-string true))
                    relationship-payload
                    {:parent_pk_namespace (get-in rel [:parent :pk_namespace])
                     :parent_pk           (get-in rel [:parent :pk])
                     :child_pk_namespace  (:pk_namespace child-entity)
                     :child_pk            (:pk child-entity)}]
                (post-to-api "/entity_relationships" relationship-payload)))))))))

(println "\nCompilation complete.")

;; Execute the main function if the script is run directly.
(when (= *file* (System/getProperty "babashka.main"))
  (-main))
