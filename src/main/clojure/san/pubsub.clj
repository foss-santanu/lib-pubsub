(ns san.pubsub 
   "The library to support pubsub mechanism")

(defprotocol pubsub-protocol
   "Defines API contracts for pubsub support"
   (start-topic [this topic-name] [this topic-name is-closed] 
                "Registers a new message topic")
   (list-topics [this] "Returns a list of all topic names registered")
   (publish-to [this topic-name topic-messg] "Publishes a new message for the topic")
   (build-topic-messg [this topic-name publisher message] "Creates a topic message to publish")
   (addto-pubsub-register [this default-callback topic-list]
                          [this default-callback pub-topics sub-topics]
                          "Registers a pubsub participant and returns a participant id")
   (subscribe-to [this topic-name participant]
                 [this topic-name participant listner-callback]
                 "Registers a participant as a listner to topic messages")
   (register-publisher [this topic-name participant] 
                       "Registers the participant as a publisher for this topic"))

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
     
     (defn- build-pubsub
        "Builds the implementation of pubsub-protocol"
        []
        (reify pubsub-protocol
              (start-topic 
                 [this topic-name] 
                 (_start-topic topic-name true))
              (start-topic 
                 [this topic-name is-closed] 
                 (_start-topic topic-name is-closed))
              (list-topics
                 [this]
                 (->> 
                      (keys @topic-queues)
                      (map #(let [[first-char & rest-chars] (str %)] 
                                 (apply str rest-chars)))
                 ))
              (publish-to 
                 [this topic-name topic-messg])
              (build-topic-messg 
                 [this topic-name publisher message])
              (addto-pubsub-register 
                 [this default-callback topic-list])
              (addto-pubsub-register 
                 [this default-callback pub-topics sub-topics])
              (subscribe-to 
                 [this topic-name participant])
              (subscribe-to 
                 [this topic-name participant listner-callback])
              (register-publisher 
                 [this topic-name participant])
        ))

     (defn pubsub
        "Returns the in memory puubsub provider"
        []
        (if (nil? pubsub-mem)
            (alter-var-root #'pubsub-mem 
                            (constantly (build-pubsub)))
        )
        pubsub-mem)
)