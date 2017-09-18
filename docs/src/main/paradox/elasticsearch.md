# Elasticsearch Connector

The Elasticsearch connector provides Akka Stream sources and sinks for Elasticsearch.

For more information about Elasticsearch please visit the [official documentation](https://www.elastic.co/guide/index.html).

## Artifacts

sbt
:   @@@vars
    ```scala
    libraryDependencies += "com.lightbend.akka" %% "akka-stream-alpakka-elasticsearch" % "$version$"
    ```
    @@@

Maven
:   @@@vars
    ```xml
    <dependency>
      <groupId>com.lightbend.akka</groupId>
      <artifactId>akka-stream-alpakka-elasticsearch_$scala.binaryVersion$</artifactId>
      <version>$version$</version>
    </dependency>
    ```
    @@@

Gradle
:   @@@vars
    ```gradle
    dependencies {
      compile group: "com.lightbend.akka", name: "akka-stream-alpakka-elasticsearch_$scala.binaryVersion$", version: "$version$"
    }
    ```
    @@@

## Usage

Sources, Flows and Sinks provided by this connector need a prepared `RestClient` to access to Elasticsearch.

Scala
: @@snip (../../../../elasticsearch/src/test/scala/akka/stream/alpakka/elasticsearch/ElasticsearchSpec.scala) { #init-client }

Java
: @@snip (../../../../elasticsearch/src/test/java/akka/stream/alpakka/elasticsearch/ElasticsearchTest.java) { #init-client }

We will also need an @scaladoc[ActorSystem](akka.actor.ActorSystem) and an @scaladoc[ActorMaterializer](akka.stream.ActorMaterializer).

Scala
: @@snip (../../../../elasticsearch/src/test/scala/akka/stream/alpakka/elasticsearch/ElasticsearchSpec.scala) { #init-mat }

Java
: @@snip (../../../../elasticsearch/src/test/java/akka/stream/alpakka/elasticsearch/ElasticsearchTest.java) { #init-mat }

This is all preparation that we are going to need.

### JsObject message

Now we can stream messages which contains spray-json's `JsObject` (in Scala) or `java.util.Map<String, Object>` (in Java) 
from or to Elasticsearch where we have access to by providing the `RestClient` to the
@scaladoc[ElasticsearchSource](akka.stream.alpakka.elasticsearch.scaladsl.ElasticsearchSource$) or the 
@scaladoc[ElasticsearchSink](akka.stream.alpakka.elasticsearch.scaladsl.ElasticsearchSink$).

Scala
: @@snip (../../../../elasticsearch/src/test/scala/akka/stream/alpakka/elasticsearch/ElasticsearchSpec.scala) { #run-jsobject }

Java
: @@snip (../../../../elasticsearch/src/test/java/akka/stream/alpakka/elasticsearch/ElasticsearchTest.java) { #run-jsobject }


### Typed messages

Also, it's possible to stream messages which contains any classes. In Scala, spray-json is used for JSON conversion, 
so defining the mapped class and `JsonFormat` for it is necessary. In Java, Jackson is used, so just define the mapped class.

Scala
: @@snip (../../../../elasticsearch/src/test/scala/akka/stream/alpakka/elasticsearch/ElasticsearchSpec.scala) { #define-class }

Java
: @@snip (../../../../elasticsearch/src/test/java/akka/stream/alpakka/elasticsearch/ElasticsearchTest.java) { #define-class }


Use `ElasticsearchSource.typed` and `ElasticsearchSink.typed` to create source and sink instead.

Scala
: @@snip (../../../../elasticsearch/src/test/scala/akka/stream/alpakka/elasticsearch/ElasticsearchSpec.scala) { #run-typed }

Java
: @@snip (../../../../elasticsearch/src/test/java/akka/stream/alpakka/elasticsearch/ElasticsearchTest.java) { #run-typed }


### Configuration

We can specify the buffer size for the source.

Scala (source)
: @@snip (../../../../elasticsearch/src/main/scala/akka/stream/alpakka/elasticsearch/scaladsl/ElasticsearchSourceSettings.scala) { #source-settings }

Also, we can specify the buffer size, the max retry count and the retry interval for the sink.

Scala (sink)
: @@snip (../../../../elasticsearch/src/main/scala/akka/stream/alpakka/elasticsearch/scaladsl/ElasticsearchSinkSettings.scala) { #sink-settings }

`ElasticsearchSource` retrieves messages from Elasticsearch by scroll scan. This buffer size is used as the scroll size.
`ElasticsearchSink` puts messages by one bulk request per messages of this buffer size.

### Using Elasticsearch as a Flow

You can also build flow stages. The API is similar to creating Sinks.

Scala (flow)
: @@snip (../../../../elasticsearch/src/test/scala/akka/stream/alpakka/elasticsearch/ElasticsearchSpec.scala) { #run-flow }

Java (flow)
: @@snip (../../../../elasticsearch/src/test/java/akka/stream/alpakka/elasticsearch/ElasticsearchTest.java) { #run-flow }

### Running the example code

The code in this guide is part of runnable tests of this project. You are welcome to edit the code and run it in sbt.

Scala
:   ```
    sbt
    > elasticsearch/testOnly *.ElasticsearchSpec
    ```

Java
:   ```
    sbt
    > elasticsearch/testOnly *.ElasticsearchTest
    ```
