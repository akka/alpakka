Alpakka [![scaladex-badge][]][scaladex] [![travis-badge][]][travis] [![gitter-badge][]][gitter]
=======

[scaladex]:       https://index.scala-lang.org/akka/alpakka
[scaladex-badge]: https://index.scala-lang.org/akka/alpakka/latest.svg
[travis]:                https://travis-ci.org/akka/alpakka
[travis-badge]:          https://travis-ci.org/akka/alpakka.svg?branch=master
[gitter]:                    https://gitter.im/akka/akka
[gitter-badge]:       https://badges.gitter.im/akka/akka.svg

Systems don't come alone. In the world of microservices, cloud deployment and services, and our history of existing solutions we need to integrate. Reactive Streams give us a technology-independent tool to let these systems communicate without overwhelming each other.

Alpakka's connectors let you stream data in and out of other technologies using Akka Streams. 

[Akka Streams](https://doc.akka.io/docs/akka/current/stream/index.html) is a [Reactive Streams](http://www.reactive-streams.org/) and JDK 9+ [java.util.concurrent.Flow](https://docs.oracle.com/javase/10/docs/api/java/util/concurrent/Flow.html)-compliant implementation and therefore [fully interoperable](https://doc.akka.io/docs/akka/current/general/stream/stream-design.html#interoperation-with-other-reactive-streams-implementations) with other implementations.


Documentation
-------------

- [Alpakka reference](http://developer.lightbend.com/docs/alpakka/current/) documentation 

- [Alpakka Kafka connector reference](https://doc.akka.io/docs/akka-stream-kafka/current/) documentation

To get a grip of the latest Alpakka releases check out [Alpakka releases](https://github.com/akka/alpakka/releases) and [Alpakka Kafka connector releases](https://github.com/akka/reactive-kafka/releases).


Community
---------

You can join these groups and chats to discuss and ask Akka and Alpakka related questions:

- Forums: [discuss.lightbend.com](https://discuss.lightbend.com/c/akka/streams-and-alpakka)
- Chat room about *using* Akka and Alpakka: [![gitter: akka/akka](https://img.shields.io/badge/gitter%3A-akka%2Fakka-blue.svg?style=flat-square)](https://gitter.im/akka/akka)
- Issue tracker: [![github: akka/alpakka](https://img.shields.io/badge/github%3A-issues-blue.svg?style=flat-square)](https://github.com/akka/alpakka/issues)

In addition to that, you may enjoy following:

- The [Akka Team Blog](https://akka.io/blog/)
- [@akkateam](https://twitter.com/akkateam) on Twitter
- Questions tagged [#alpakka on StackOverflow](http://stackoverflow.com/questions/tagged/alpakka)

[Lightbend](https://www.lightbend.com/) is committed to Alpakka and has an Alpakka team working on it.


Contributing
------------

Contributions are *very* welcome! Alpakka loves new maintainers as well as old maintainers.

There are more technologies to integrate with than a single person possibly could keep track of. That is why Alpakka is so dependent on its community to develop and to keep after all these connectors. Please step up and share the successful Akka Stream integrations you implement with the Alpakka community.

If you see an issue that you'd like to see fixed, the best way to make it happen is to help out by submitting a pull request implementing it.

Refer to the [CONTRIBUTING.md](CONTRIBUTING.md) file for more details about the workflow, and general hints on how to prepare your pull request. If you're planning to implement a new module within Alpakka, have a look at our [contributor advice](contributor-advice.md) to have a good start.

You can also ask for clarifications or guidance in GitHub issues directly, or in the [akka/dev](https://gitter.im/akka/dev) chat if a more real time communication would be of benefit.



Caveat Emptor
-------------

Alpakka components do not have to obey the rule of staying binary compatible between releases. Breaking API changes may be introduced without notice as we refine and simplify based on your feedback. A module may be dropped in any release without prior deprecation. If not stated otherwise, the [Lightbend subscription](https://www.lightbend.com/subscription) does *not* cover support for Alpakka modules.

Having that said, we aim to move Alpakka to a state where APIs are stable and even more well-tested.
