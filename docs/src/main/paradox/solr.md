# Apache Solr Connector

The Solr connector provides Akka Stream sources and sinks for Solr.

For more information about Solr please visit the [Solr documentation](http://lucene.apache.org/solr/resources.html).


### Reported issues

[Tagged issues at Github](https://github.com/akka/alpakka/labels/p%3Asolr)


## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-solr_$scalaBinaryVersion$
  version=$version$
}

## Set up client

Sources, Flows and Sinks provided by this connector need a prepared `org.apache.solr.client.solrj.SolrClient` to
access to Solr.


Scala
: @@snip ($alpakka$/solr/src/test/scala/akka/stream/alpakka/solr/SolrSpec.scala) { #init-client }

Java
: @@snip ($alpakka$/solr/src/test/java/akka/stream/alpakka/solr/SolrTest.java) { #init-client }


## Source Usage

Create a tuple stream.

Scala
: @@snip ($alpakka$/solr/src/test/scala/akka/stream/alpakka/solr/SolrSpec.scala) { #tuple-stream }

Java
: @@snip ($alpakka$/solr/src/test/java/akka/stream/alpakka/solr/SolrTest.java) { #tuple-stream }


Use `SolrSource.create` to create 
@scala[@scaladoc[SolrSource](akka.stream.alpakka.solr.scaladsl.SolrSource$).]
@java[@scaladoc[SolrSource](akka.stream.alpakka.solr.javadsl.SolrSource$).]


Scala
: @@snip ($alpakka$/solr/src/test/scala/akka/stream/alpakka/solr/SolrSpec.scala) { #define-source }

Java
: @@snip ($alpakka$/solr/src/test/java/akka/stream/alpakka/solr/SolrTest.java) { #define-source }

## Sink Usage

Now we can stream messages to Solr by providing the `SolrClient` to the
@scala[@scaladoc[SolrSink](akka.stream.alpakka.solr.scaladsl.SolrSink$).]
@java[@scaladoc[SolrSink](akka.stream.alpakka.solr.javadsl.SolrSink$).]


Scala
: @@snip ($alpakka$/solr/src/test/scala/akka/stream/alpakka/solr/SolrSpec.scala) { #define-class }

Java
: @@snip ($alpakka$/solr/src/test/java/akka/stream/alpakka/solr/SolrTest.java) { #define-class }

### With document sink

Use `SolrSink.document` to stream `SolrInputDocument` to Solr.


Scala
: @@snip ($alpakka$/solr/src/test/scala/akka/stream/alpakka/solr/SolrSpec.scala) { #run-document }

Java
: @@snip ($alpakka$/solr/src/test/java/akka/stream/alpakka/solr/SolrTest.java) { #run-document }

### With bean sink

Firstly, create a POJO.

Scala
: @@snip ($alpakka$/solr/src/test/scala/akka/stream/alpakka/solr/SolrSpec.scala) { #define-bean }

Java
: @@snip ($alpakka$/solr/src/test/java/akka/stream/alpakka/solr/SolrTest.java) { #define-bean }

Use `SolrSink.bean` to stream POJOs to Solr.


Scala
: @@snip ($alpakka$/solr/src/test/scala/akka/stream/alpakka/solr/SolrSpec.scala) { #run-bean }

Java
: @@snip ($alpakka$/solr/src/test/java/akka/stream/alpakka/solr/SolrTest.java) { #run-bean }

### With typed sink

Use `SolrSink.typed` to stream messages with custom binding to Solr.


Scala
: @@snip ($alpakka$/solr/src/test/scala/akka/stream/alpakka/solr/SolrSpec.scala) { #run-typed }

Java
: @@snip ($alpakka$/solr/src/test/java/akka/stream/alpakka/solr/SolrTest.java) { #run-typed }

### Configuration

We can configure the sink by `SolrUpdateSettings`.


Scala
: @@snip ($alpakka$/solr/src/test/scala/akka/stream/alpakka/solr/SolrSpec.scala) { #solr-update-settings }

Java
: @@snip ($alpakka$/solr/src/test/java/akka/stream/alpakka/solr/SolrTest.java) { #solr-update-settings }


| Parameter           | Default | Description                                                                                            |
| ------------------- | ------- | ------------------------------------------------------------------------------------------------------ |
| bufferSize          | 10      | `SolrSink` puts messages by one bulk request per messages of this buffer size.                |
| retryInterval       | 5000    | When a request is failed, `SolrSink` retries that request after this interval (milliseconds). |
| maxRetry            | 100     | `SolrSink` give up and fails the stage if it gets this number of consective failures.         | 
| commitWithin        | -1      | Max time (in ms) before a commit will happen, -1 for manual committing |

## Flow Usage

You can also build flow stages with
@scala[@scaladoc[SolrFlow](akka.stream.alpakka.solr.scaladsl.SolrFlow$).]
@java[@scaladoc[SolrFlow](akka.stream.alpakka.solr.javadsl.SolrFlow$).]
The API is similar to creating Sinks.

Scala
: @@snip ($alpakka$/solr/src/test/scala/akka/stream/alpakka/solr/SolrSpec.scala) { #run-flow }

Java
: @@snip ($alpakka$/solr/src/test/java/akka/stream/alpakka/solr/SolrTest.java) { #run-flow }

### Passing data through SolrFlow

Use `SolrFlow.documentWithPassThrough`, `SolrFlow.beanWithPassThrough` or `SolrFlow.typedWithPassThrough`.

When streaming documents from Kafka, you might want to commit to Kafka **AFTER** the document has been written to Solr.

Scala
: @@snip ($alpakka$/solr/src/test/scala/akka/stream/alpakka/solr/SolrSpec.scala) { #kafka-example }

Java
: @@snip ($alpakka$/solr/src/test/java/akka/stream/alpakka/solr/SolrTest.java) { #kafka-example }


### Running the example code

The code in this guide is part of runnable tests of this project. You are welcome to edit the code and run it in sbt.

Scala
:   ```
    sbt
    > solr/testOnly *.SolrSpec
    ```

Java
:   ```
    sbt
    > solr/testOnly *.SolrTest
    ```
