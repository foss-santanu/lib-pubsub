(ns san.test-pubsub
   (:use clojure.test)
   (:use san.pubsub :reload))

(deftest test-pubsub-singleton
   (testing "pubsub provider is not nil"
      (->>
           (pubsub)
           nil?
           not
           is)
   )
   (testing "pubsub provider type is pubsub-protocol"
      (->>
           (pubsub)
           (satisfies? pubsub-protocol)
           is)
   ))

;;; A macro to test private functions from the package
;;; courtesy http://nakkaya.com/2009/11/18/unit-testing-in-clojure/
(defmacro with-private-fns [[ns fns] & tests]
  "Refers private fns from ns and runs tests in context."
  `(let ~(reduce #(conj %1 %2 `(ns-resolve '~ns '~%2)) [] fns)
     ~@tests))

(with-private-fns [san.pubsub [build-add-q]]
   (deftest build-add-q-topic
      (testing "build-add-q when topic already exists"
         (->>
              (build-add-q {:test_topic {}} "test_topic" true)
              :test_topic
              empty?
              is)
      ))
   (deftest build-add-q-notopic
      (testing "build-add-q when new open/closed topic"
         (->>
              (build-add-q {} "test_topic" true)
              :test_topic
              deref
              empty?
              not
              is)
         (->>
              (build-add-q {} "test_topic" true)
              :test_topic
              deref
              :name
              (= "test_topic")
              is)
         (->>
              (build-add-q {} "test_topic" true)
              :test_topic
              deref
              :queue
              sorted?
              is)
         (->>
              (build-add-q {} "test_topic" true)
              :test_topic
              deref
              :subscribers
              vector?
              is)
         (->>
              (build-add-q {} "test_topic" true)
              :test_topic
              deref
              :publishers
              vector?
              is)
         (->>
              (build-add-q {} "test_topic" false)
              :test_topic
              deref
              :subscribers
              (= "all")
              is)
         (->>
              (build-add-q {} "test_topic" false)
              :test_topic
              deref
              :publishers
              (= "any")
              is)
      ))
   )


(run-tests 'san.test-pubsub)