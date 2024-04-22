Alpakka
=======

Systems don't come alone. In the modern world of microservices and cloud deployment, new components must interact with legacy systems, making integration an important key to success. Reactive Streams give us a technology-independent tool to let these heterogeneous systems communicate without overwhelming each other.

The Alpakka project implements stream-aware & reactive integration pipelines for Java and Scala. It is built on top of [Akka Streams](https://doc.akka.io/docs/akka/current/stream/index.html), and has been designed from the ground up to understand streaming natively and provide a DSL for reactive and stream-oriented programming, with built-in support for backpressure. Akka Streams is a [Reactive Streams](http://www.reactive-streams.org/) and JDK 9+ [java.util.concurrent.Flow](https://docs.oracle.com/javase/10/docs/api/java/util/concurrent/Flow.html)-compliant implementation and therefore [fully interoperable](https://doc.akka.io/docs/akka/current/general/stream/stream-design.html#interoperation-with-other-reactive-streams-implementations) with other implementations.

The Akka family of projects is managed by teams at [Lightbend](https://lightbend.com) with help from the community.

Documentation
-------------

- [Alpakka reference](https://doc.akka.io/docs/alpakka/current/) documentation

- [Alpakka Kafka connector reference](https://doc.akka.io/docs/akka-stream-kafka/current/) documentation

To keep up with the latest Alpakka releases check out [Alpakka releases](https://github.com/akka/alpakka/releases) and [Alpakka Kafka connector releases](https://github.com/akka/alpakka-kafka/releases).


Community
---------

You can join these forums and chats to discuss and ask Akka and Alpakka related questions:

- Forums: [discuss.lightbend.com](https://discuss.lightbend.com/c/akka/streams-and-alpakka)
- Issue tracker: [github.com/akka/alpakka/issues](https://github.com/akka/alpakka/issues)

In addition to that, you may enjoy the following:

- The [Akka Team Blog](https://akka.io/blog/)
- [@akkateam](https://twitter.com/akkateam) on Twitter
- Questions tagged [#alpakka on StackOverflow](http://stackoverflow.com/questions/tagged/alpakka)



Contributing
------------

[Lightbend](https://www.lightbend.com/) is committed to Alpakka and has an Alpakka team working on it.

Contributions are *very* welcome! The Alpakka team appreciates community contributions by both those new to Alpakka and those more experienced.
Alpakka depends on the community to keep up with the ever-growing number of technologies with which to integrate. Please step up and share the successful Akka Stream integrations you implement with the Alpakka community.

If you find an issue that you'd like to see fixed, the quickest way to make that happen is to implement the fix and submit a pull request.

Refer to the [CONTRIBUTING.md](CONTRIBUTING.md) file for more details about the workflow, and general hints on how to prepare your pull request. If you're planning to implement a new module within Alpakka, look at our [contributor advice](contributor-advice.md).

You can also ask for clarifications or guidance in GitHub issues directly, or in the [akka/dev](https://gitter.im/akka/dev) chat if a more real time communication would be of benefit.

Caveat Emptor
-------------

Alpakka components are not always binary compatible between releases. API changes that are not backward compatible might be introduced as we refine and simplify based on your feedback. A module may be dropped in any release without prior deprecation. If not stated otherwise, the [Lightbend subscription](https://www.lightbend.com/subscription) does *not* cover support for Alpakka modules.

Our goal is to improve the stability and test coverage for Alpakka APIs over time.

License
-------
Alpakka is licensed under the [Business Source License (BSL) 1.1](https://github.com/akka/alpakka/blob/main/LICENSE), please see the [Akka License FAQ](https://www.lightbend.com/akka/license-faq).
