;;; This defines the interface for a pubsub provider
;;; Author: Santanu Chakrabarti
;;; Email to: santanu.chakrabarti@gmail.com
;;; Date: 24/04/2015 1:26 AM

(ns san.pubsub-api
   "Interface for a pubsub provider")

(defprotocol pubsub-provider
   "Defines API contracts for pubsub support"
   (start-topic [this topic-name] [this topic-name is-closed] 
                "Registers a new message topic")
   (list-topics [this] "Returns a list of all topic names registered")
   (publish-to [this topic-name publisher message] 
               "Publishes a new message for the topic")
   (read-message [this topic-name message-id] "Returns the message payload for the id")
   (read-all-from [this topic-name message-id] 
                  "Returns all message payloads starting from the message id")
   (addto-pubsub-register [this default-callback topic-list]
                          [this default-callback pub-topics sub-topics]
                          "Registers a pubsub participant and returns a participant id")
   (removefrom-pubsub-register [this participant] 
                               "No longer can participate in any pubsub activity")
   (subscribe-to [this topic-name participant]
                 [this topic-name participant listner-callback]
                 "Registers a participant as a listner to topic messages")
   (unsubscribe-from [this topic-name participant] 
                     "Unsubscribe the participant from listening the topic")
   (register-publisher [this topic-name participant] 
                       "Registers the participant as a publisher for this topic")
   (remove-publisher [this topic-name participant] 
                     "The participant is no longer a publisher for this closed topic"))

(defmulti build-pubsub
   "The factory for a pubsub provider"
   (fn [x] x))

(defmethod build-pubsub :default
   [provider-type]
   (throw (IllegalAccessException. 
             (str "Factory for " provider-type " provider not implemented currently"))))
