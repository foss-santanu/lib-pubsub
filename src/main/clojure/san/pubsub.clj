(ns san.pubsub 
   "The library to support pubsub mechanism"
   (:use san.pubsub-api :reload))

(def ^{:private true :dynamic true} pubsub-mem nil)

(let [ pubsub-register (atom {}) 
       topic-queues (atom {})
       ;;; Small helper functions - kept opaque to the package
       subscriber-exists? (fn [topic subscriber]
                              (let [subs (get-in @topic-queues [topic :subscribers])]
                                (if (and (string? subs) (= subs "all")) 
                                    true 
                                    (some #(= subscriber %) (seq subs))
                                )
                              ))
       can-publish-to? (fn [topic publisher]
                              (let [pubs (get-in @topic-queues [topic :publishers])]
                                (if (and (string? pubs) (= pubs "any")) 
                                    true 
                                    (some #(= publisher %) (seq pubs))
                                )
                              ))
     ]
     
     (defn- topic-error-handler
        "Processes exceptions in a topic queue"
        [topic error]
        ;; TODO - currently only prints the error message
        (println "Exception @{topic: " (:name @topic) "}: " error))
     
     (defn- build-add-q
        "Checks if topic queue already exists else create one"
        [qmap topic-name is-closed]
        (if-not ((keyword topic-name) qmap)          
           (->> 
                (->  
                    (->> (sorted-map)
                         (assoc {} :queue))
                    (assoc :name topic-name)
                    (assoc :subscribers (if is-closed [] "all"))
                    (assoc :publishers (if is-closed [] "any"))
                    (agent :error-handler topic-error-handler)
                )
                (assoc qmap (keyword topic-name))
           )
           qmap
        ))
     
     (defn- _start-topic
        "Function that takes the load of building a new topic"
        [topic-name is-closed]
        ;; TODO - check topic-name does not contain space, /, etc.
        (swap! topic-queues build-add-q topic-name is-closed))
     
     (defmethod build-pubsub :in-memory
        [provider-type]
        (reify pubsub-provider
              (start-topic 
                 [this topic-name] 
                 (_start-topic topic-name true)
                 this)
              (start-topic 
                 [this topic-name is-closed] 
                 (_start-topic topic-name is-closed)
                 this)
              (list-topics
                 [this]
                 (->> 
                      (keys @topic-queues)
                      (map #(let [[first-char & rest-chars] (str %)] 
                                 (apply str rest-chars)))
                 ))
              (publish-to 
                 [this topic-name publisher message])
              (read-message 
                 [this topic-name message-id])
              (read-all-from 
                 [this topic-name message-id])
              (addto-pubsub-register 
                 [this default-callback topic-list])
              (addto-pubsub-register 
                 [this default-callback pub-topics sub-topics])
              (removefrom-pubsub-register 
                 [this participant])
              (subscribe-to 
                 [this topic-name participant])
              (subscribe-to 
                 [this topic-name participant listner-callback])
              (unsubscribe-from 
                 [this topic-name participant])
              (register-publisher 
                 [this topic-name participant])
              (remove-publisher 
                 [this topic-name participant]) 
        ))

     (defn pubsub
        "Returns the in memory puubsub provider"
        []
        (if (nil? pubsub-mem)
            (alter-var-root #'pubsub-mem 
                            (constantly (build-pubsub :in-memory)))
        )
        pubsub-mem)
)