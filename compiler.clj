#!/usr/bin/env bb

;; compiler.clj
;; Reads all .entity and .relationship files in the repository, resolves globs,
;; and pushes the resulting graph to the Sorter API.

(require '[babashka.fs :as fs]
         '[cheshire.core :as json]
         '[babashka.process :as p])

;; --- Configuration ---
;; These are read from environment variables for security.
(def SORTER_API_BASE_URL (System/getenv "SORTER_API_URL"))
(def SORTER_API_KEY (System/getenv "SORTER_API_KEY"))

;; Helper function to make authenticated POST requests to the API.
(defn post-to-api [endpoint data]
  (println "POST ->" endpoint)
  (let [payload (json/generate-string data)
        ;; Use babashka.process/shell to call curl. It handles arguments safely.
        result (p/shell {:out :string :err :string}
                        "curl" "-s" "-X" "POST"
                        "-H" "Content-Type: application/json"
                        "-H" (str "Authorization: Bearer " SORTER_API_KEY)
                        (str SORTER_API_BASE_URL endpoint)
                        "-d" payload)]
    (when (not (zero? (:exit result)))
      (println "  ERROR:" (:err result)))))

;; --- The Compiler Logic ---

(defn -main [& args]
  (when (or (empty? SORTER_API_BASE_URL) (empty? SORTER_API_KEY))
    (println "ERROR: SORTER_API_URL and SORTER_API_KEY environment variables must be set.")
    (System/exit 1))

  ;; --- PASS 1: Ingest all atomic entities (Nodes) ---
  ;; This ensures all potential children exist before we create relationships.
  (println "\n--- PASS 1: Ingesting Entities ---")
  (let [entity-files (fs/glob "." "**/*.entity")]
    (doseq [file entity-files]
      (-> file slurp (json/parse-string true) (post-to-api "/entity"))))

  ;; --- PASS 2: Ingest all relationships (Edges) ---
  ;; This creates the parent "collection" entities and wires up the children.
  (println "\n--- PASS 2: Ingesting Relationships ---")
  (let [relationship-files (fs/glob "." "**/*.relationship")]
    (doseq [rel-file relationship-files]
      (let [relationships (-> rel-file slurp (json/parse-string true))]
        (doseq [rel relationships]
          ;; First, create the parent "collection" entity itself.
          (post-to-api "/entity" (:parent rel))

          ;; Then, resolve the children via glob and create the links.
          (doseq [glob-pattern (:children_glob rel)]
            (let [child-files (fs/glob (fs/parent rel-file) glob-pattern)]
              (doseq [child-file child-files]
                (let [child-entity (-> child-file slurp (json/parse-string true))
                      relationship-payload
                      {:parent_pk_namespace (get-in rel [:parent :pk_namespace])
                       :parent_pk           (get-in rel [:parent :pk])
                       :child_pk_namespace  (:pk_namespace child-entity)
                       :child_pk            (:pk child-entity)}]
                  (post-to-api "/entity_relationships" relationship-payload)))))))))

  (println "\nCompilation complete."))

;; Execute the main function if the script is run directly.
(when (= *file* (System/getProperty "babashka.main"))
  (-main))
