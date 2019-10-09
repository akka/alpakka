# Overview

The [Alpakka project](https://doc.akka.io/docs/alpakka/current/) is an open source initiative to implement stream-aware and reactive integration pipelines for Java and Scala. It is built on top of [Akka Streams](https://doc.akka.io/docs/akka/current/stream/index.html), and has been designed from the ground up to understand streaming natively and provide a DSL for reactive and stream-oriented programming, with built-in support for backpressure. Akka Streams is a [Reactive Streams](https://www.reactive-streams.org/) and JDK 9+ [java.util.concurrent.Flow](https://docs.oracle.com/javase/10/docs/api/java/util/concurrent/Flow.html)-compliant implementation and therefore [fully interoperable](https://doc.akka.io/docs/akka/current/general/stream/stream-design.html#interoperation-with-other-reactive-streams-implementations) with other implementations.

If you'd like to know what integrations with Alpakka look like, have a look at our 
@ref[self-contained examples](examples/index.md) section.

There are a few blog posts and presentations about Alpakka out there, we've @ref[collected some](other-docs/webinars-presentations-articles.md).


## Versions

The code in this documentation is compiled against

* Alpakka $project.version$ ([Github](https://github.com/akka/alpakka), [API docs](https://doc.akka.io/api/alpakka/current/akka/stream/alpakka/index.html))
* Scala $scala.binary.version$ (most modules are available for Scala 2.13, and almost all are available for Scala 2.11)
* Akka Streams $akka.version$ (@extref:[Reference](akka:stream/index.html), [Github](https://github.com/akka/akka))
* Akka Http $akka-http.version$ (@extref:[Reference](akka-http:), [Github](https://github.com/akka/akka-http))

Release notes are found at @ref:[Release Notes](release-notes/index.md).

If you want to try out a connector that has not yet been released, give @ref[snapshots](other-docs/snapshots.md) a spin which are published after every merged PR.

## Contributing

Please feel free to contribute to Alpakka by reporting issues you identify, or by suggesting changes to the code. Please refer to our [contributing instructions](https://github.com/akka/alpakka/blob/master/CONTRIBUTING.md) and our [contributor advice](https://github.com/akka/alpakka/blob/master/contributor-advice.md) to learn how it can be done. The target structure for Alpakka connectors is illustrated by the @ref[Reference connector](reference.md).

We want Akka and Alpakka to strive in a welcoming and open atmosphere and expect all contributors to respect our [code of conduct](https://www.lightbend.com/conduct).

[![alpakka]][alpakka-scaladex] Feel free to tag your project with *akka-streams* keyword in Scaladex for easier discoverability.

[alpakka]: https://index.scala-lang.org/count.svg?q=topics:akka-streams&amp;subject=akka-streams&amp;style=flat-square

[alpakka-scaladex]: https://index.scala-lang.org/search?q=topics:akka-streams


@@ toc { .main depth=2 }

@@@ index

* [External stream components](external-components.md) (hosted separately)
* [Self-contained examples](examples/index.md)
* [Other documentation resources](other-docs/index.md)
* [Integration Patterns](patterns.md)
* [Release notes](release-notes/index.md)

@@@
