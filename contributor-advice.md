# Some advice for Alpakka contributors

## Reference connector

To inspect how all of the below listed guidelines are to be implemented in practice, take a look at
[the reference](reference/) connector. Feel free to use the reference connector as a starting point for new connectors.

## API design

### Public factory methods

Depending on the technology you integrate with Akka Streams and Alpakka you'll create Sources, Flows and Sinks.
Regardless on how they are implemented make sure that you create the relevant Sources, Sinks and Flows APIs so they are
simple and easy to use.

### Flows

> Reference connector [model classes](reference/src/main/scala/akka/stream/alpakka/reference/model.scala)

When designing Flows, consider adding an extra field to the in- and out-messages which is passed through. A common
use case we see, is committing a Kafka offset after passing data to another system.

### Java APIs

> Reference connector [Java API factory methods](reference/src/main/scala/akka/stream/alpakka/javadsl/Reference.scala)

Alpakka, same as Akka, aims to keep 100% feature parity between the various language DSLs.

* Provide factory methods for Sources, Flows and Sinks in the `javadsl` package wrapping all the methods in the Scala API
* The Akka Stream Scala instances have a `.asJava` method to convert to the `akka.stream.javadsl` counterparts
* Let the Java API use Java standard types (`java.util.Optional`, `java.util.List`, ...) and convert using
`JavaConverters` where needed
* Use the `akka.japi.Pair` class to return tuples
* Use `scala.compat.java8.FutureConverters` to translate Futures to `CompletionStage`s

### Settings

> Reference connector [settings classes](reference/src/main/scala/akka/stream/alpakka/reference/settings.scala)

Most technologies will have a couple of configuration settings that will be needed for several Sinks, Flows, or Sinks. 
Create case classes collecting these settings instead of passing them in every method.

Create a default instance in the companion object with good defaults which can be updated via `withXxxx` methods.

Add `withXxxx` methods to specify certain fields in the settings instance.

In case you see the need to support reading the settings from `Config`, offer a method taking the `Config` instance so
that the user can apply a proper namespace.
Refrain from using `akka.stream.alpakka` as Config prefix, prefer `alpakka` as root namespace.

## Implementation details

### External Dependencies

All the external runtime dependencies for the project, including transitive dependencies, must have an open source license that is equal to, or compatible with, [Apache 2](http://www.apache.org/licenses/LICENSE-2.0).

Which licenses are compatible with Apache 2 are defined in [this doc](http://www.apache.org/legal/3party.html#category-a), where you can see that the licenses that are listed under ``Category A`` automatically compatible with Apache 2, while the ones listed under ``Category B`` needs additional action:

> Each license in this category requires some degree of [reciprocity](http://www.apache.org/legal/3party.html#define-reciprocal); therefore, additional action must be taken in order to minimize the chance that a user of an Apache product will create a derivative work of a reciprocally-licensed portion of an Apache product without being aware of the applicable requirements.

Dependency licenses will be checked automatically by the sbt Whitesource plug-in. 

### Packages & Scoping

Use `private`, `private[connector]` and `final` extensively to limit the API surface.

| Package                                  | Purpose
| -----------------------------------------|------------------------
| `akka.stream.alpakka.connector.javadsl`  | Java-only part of the API, normally factories for Sources, Flows and Sinks
| `akka.stream.alpakka.connector.scaladsl` | Scala-only part of the API, normally factories for Sources, Flows and Sinks
| `akka.stream.alpakka.connector`          | Shared API, eg. settings classes
| `akka.stream.alpakka.connector.impl`     | Implementation in separate package

### Graph stage checklist

> Reference connector [operator implementations](reference/src/main/scala/akka/stream/alpakka/reference/impl/)

* Keep mutable state within the `GraphStageLogic` only 
* Open connections in `preStart`
* Release resources in `postStop`
* Fail early on configuration errors
* Make sure the code is thread-safe; if in doubt, please ask!
* No Blocking At Any Time -- in other words, avoid blocking whenever possible and replace it with asynchronous 
programming (async callbacks, stage actors)

### Keep the code DRY

Avoid duplication of code between different Sources, Sinks and Flows. Extract the common logic to a common abstract
base `GraphStageLogic` that is inherited by the `GraphStage`s.

Sometimes it may be useful to provide a Sink or Source for a connector, even if the main concept is implemented
as a Flow. This can be easily done by reusing the Flow implementation:
* Source: `Source.maybe.viaMat(MyFlow)(Keep.right)`
* Sink: `MyFlow.toMat(Sink.ignore)(Keep.right)`

You do not need to expose every configuration a Flow offers this way -- Focus on the most important ones.

## Test

> Reference connector [Scala](reference/src/test/scala/docs/scaladsl/ReferenceSpec.scala) and
> [Java](reference/src/test/java/docs/javadsl/ReferenceTest.java) tests

Write tests for all public methods and possible settings.

Use Docker containers for any testing needed by your application. The container's configuration can be added to the
`docker-compose.yml` file.
Please ensure that you limit the amount of resources used by the containers.

## Documentation

> Reference connector [paradox documentation](docs/src/main/paradox/refernece.md)

Using [Paradox](https://github.com/lightbend/paradox) syntax (which is very close to markdown), create or complement
the documentation in the `docs` module.
Prepare code snippets to be integrated by Paradox in the tests. Such example should be part of real tests and not in
unused methods.

Use ScalaDoc if you see the need to describe the API usage better than the naming does.

Run `sbt docs/Local/paradox` to generate reference docs while developing. Generated documentation can be 
found in the `./docs/target/paradox/site/local` directory.
