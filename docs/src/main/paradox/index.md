# Alpakka

Welcome to the home of the Alpakka initiative, which harbours various Akka Streams connectors, integration patterns,
and data transformations for integration use cases. Here you can find documentation of the components that are
part of this project as well as links to components that are maintained by other projects.

If you'd like to know what integrations with Alpakka look like, have a look at our 
@ref[self-contained examples](examples/index.md) section.

There are a few blog posts and presentations about Alpakka out there, we've @ref[collected some](other-docs/webinars-presentations-articles.md).

The code in this documentation is compiled against

* Alpakka $version$ ([Github](https://github.com/akka/alpakka), [API docs](https://developer.lightbend.com/docs/api/alpakka/current/akka/stream/alpakka/index.html))
* Scala $scalaBinaryVersion$ (also available for Scala 2.11)
* Akka Streams $akkaVersion$ (@extref[Docs](akka-docs:stream/index.html), [Github](https://github.com/akka/akka))
* Akka Http $akkaHttpVersion$ (@extref[Docs Scala](akka-http-docs:scala.html), @extref[Docs Java](akka-http-docs:java.html), [Github](https://github.com/akka/akka-http))

Release notes are found at [Github releases](https://github.com/akka/alpakka/releases).

If you want to try out a connector that has not yet been released, give @ref[snapshots](other-docs/snapshots.md) a spin which are published after every merged PR.

## Contributing

Please feel free to contribute to Alpakka by reporting issues you identify, or by suggesting changes to the code. Please refer to our [contributing instructions](https://github.com/akka/alpakka/blob/master/CONTRIBUTING.md) and our [contributor advice](https://github.com/akka/alpakka/blob/master/contributor-advice.md) to learn how it can be done. The target structure for Alpakka connectors is illustrated by the @ref[Reference connector](reference.md).

We want Akka and Alpakka to strive in a welcoming and open atmosphere and expect all contributors to respect our [code of conduct](https://github.com/akka/alpakka/blob/master/CODE_OF_CONDUCT.md).


@@ toc { .main depth=2 }

@@@ index

* [Connectors](connectors.md)
* [External stream components](external-components.md) (hosted separately)
* [Self-contained examples](examples/index.md)
* [Other documentation resources](other-docs/index.md)
* [Integration Patterns](patterns.md)
* [Data Transformations](data-transformations/index.md)

@@@
