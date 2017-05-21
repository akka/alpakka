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

Now we can stream messages which contains spray-json's `JsObject` from or to Elasticsearch where we have access to by providing the `RestClient` to the
@scaladoc[ElasticsearchSource](akka.stream.alpakka.elasticsearch.scaladsl.ElasticsearchSource$) or the 
@scaladoc[ElasticsearchSink](akka.stream.alpakka.elasticsearch.scaladsl.ElasticsearchSink$).

Scala
: @@snip (../../../../elasticsearch/src/test/scala/akka/stream/alpakka/elasticsearch/ElasticsearchSpec.scala) { #run-jsobject }

Java
: @@snip (../../../../elasticsearch/src/test/java/akka/stream/alpakka/elasticsearch/ElasticsearchTest.java) { #run-jsobject }


### Typed messages

Also, it's possible to stream messages which contains any classes by defining `JsonFormat`.

Scala
: @@snip (../../../../elasticsearch/src/test/scala/akka/stream/alpakka/elasticsearch/ElasticsearchSpec.scala) { #define-jsonformat }

Use `ElasticsearchSource.typed` and `ElasticsearchSink.typed` to create source and sink instead.

Scala
: @@snip (../../../../elasticsearch/src/test/scala/akka/stream/alpakka/elasticsearch/ElasticsearchSpec.scala) { #run-typed }

In Java, import @scaladoc[SprayJsonSupport](akka.stream.alpakka.elasticsearch.javadsl.SprayJsonSupport$) members statically to use spray-json easily.

Java
: @@snip (../../../../elasticsearch/src/test/java/akka/stream/alpakka/elasticsearch/ElasticsearchTest.java) { #import-spray-json-support }

and define `JsonFormat` as following:

Java
: @@snip (../../../../elasticsearch/src/test/java/akka/stream/alpakka/elasticsearch/ElasticsearchTest.java) { #define-jsonformat }

Then we can stream typed messages in Java.

Java
: @@snip (../../../../elasticsearch/src/test/java/akka/stream/alpakka/elasticsearch/ElasticsearchTest.java) { #run-typed }


### Configuration

We can specify the buffer size for the source and the sink.

Scala (source)
: @@snip (../../../../elasticsearch/src/main/scala/akka/stream/alpakka/elasticsearch/ElasticsearchSourceStage.scala) { #source-settings }

Scala (sink)
: @@snip (../../../../elasticsearch/src/main/scala/akka/stream/alpakka/elasticsearch/ElasticsearchFlowStage.scala) { #sink-settings }

`ElasticsearchSource` retrieves messages from Elasticsearch by scroll scan. This buffer size is used as the scroll size.
`ElasticsearchSink` puts messages by one bulk request per messages of this buffer size.

### Using Elasticsearch as a Flow

You can also build flow stages which put or acknowledge messages in Elasticsearch, backpressure on queue response and then forward
responses further down the stream. The API is similar to creating Sinks.

Scala (flow)
: TBA

Java (flow)
: TBA

Scala (flow with ack)
: TBA

Java (flow with ack)
: TBA

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
