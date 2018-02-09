# Some advice for Alpakka contributors

## API design

Depending on the technology you integrate with Akka Streams and Alpakka you'll create Sources, Flows and Sinks.

When designing Sinks, please check if a Flow might be a better fit. Most other systems will give some kind of response which
could continue in a Flow. Sinks can easily created be attaching a Sink.ignore to the Flow. In case you want to offer 
Sinks based on Flows, focus on the most important cases and leave the more intricate variants just in the Flow API.

When designing Flows, consider adding an extra field to the in- and out-messages which is passed through. A common
use case we see, is committing a Kafka offset after passing data to another system.


### Settings

Most technologies will have a couple of configuration settings that will be needed for all Flows. Create case classes
collecting these settings instead of passing them in every method.

Create a default instance in the companion object with good defaults which can be updated via `copy` or `withXxxx` methods.

As Java can't use the `copy` methods, add `withXxxx` methods to specify certain fields in the settings instance. 

In case you see the need to support reading the settings from `Config`, offer a method taking the `Config` instance so
that the user can apply a proper namespace.
Refrain from using `akka.stream` as prefix, prefer `alpakka` as root namespace.


### Packages & Scoping

Use `private`, `private[connector]` and `final` extensively to limit the API surface. 

| Package                                  | Purpose
| -----------------------------------------|------------------------
| `akka.stream.alpakka.connector.javadsl`  | Java-only part of the API, normally factories for Sources, Flows and Sinks
| `akka.stream.alpakka.connector.scaladsl` | Scala-only part of the API, normally factories for Sources, Flows and Sinks
| `akka.stream.alpakka.connector`          | Shared API, eg. settings classes, implementation if thorowly made private
| `akka.stream.alpakka.connector.impl`     | Optional, implementation in separate package


### Regarding Java APIs

* Provide factory methods for Sources, Flows and Sinks in the `javadsl` package
* The Akka Stream Scala instances have a `.asJava` method to convert to the `akka.stream.javadsl` counterparts
* Let the Java API use Java standard types (`java.util.Optional`, `java.util.List`, ...) and convert using 
`JavaConverters` where needed
* Use the `akka.japi.Pair` class to return tuples
* Use `scala.compat.java8.FutureConverters` to translate Futures to `CompletionStage`s


## Graph stage checklist

* Keep mutable state within the `GraphStageLogic` only
* Open/close connections in `preStart` 
* Release resources in `postStop`
* Fail early on configuration errors

## Documentation

Create or complement the documentation in the `docs` module using [Paradox](https://github.com/lightbend/paradox) syntax.
Prepare code snippets to be integrated by Paradox in the tests.

Use ScalaDoc if you see the need to describe the API usage better than the naming does. 