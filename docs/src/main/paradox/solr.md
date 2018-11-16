# Apache Solr

The Solr connector provides Akka Stream sources and sinks for Solr.

For more information about Solr please visit the [Solr documentation](http://lucene.apache.org/solr/resources.html).

### Reported issues

[Tagged issues at Github](https://github.com/akka/alpakka/labels/p%3Asolr)

## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-solr_$scala.binary.version$
  version=$project.version$
}

## Set up client

Sources, Flows and Sinks provided by this connector need a prepared `org.apache.solr.client.solrj.SolrClient` to
access to Solr.


Scala
: @@snip [snip](/solr/src/test/scala/akka/stream/alpakka/solr/SolrSpec.scala) { #init-client }

Java
: @@snip [snip](/solr/src/test/java/akka/stream/alpakka/solr/SolrTest.java) { #init-client }


## Source Usage

Create a tuple stream.

Scala
: @@snip [snip](/solr/src/test/scala/akka/stream/alpakka/solr/SolrSpec.scala) { #tuple-stream }

Java
: @@snip [snip](/solr/src/test/java/akka/stream/alpakka/solr/SolrTest.java) { #tuple-stream }


Use `SolrSource.create` to create 
@scala[@scaladoc[SolrSource](akka.stream.alpakka.solr.scaladsl.SolrSource$).]
@java[@scaladoc[SolrSource](akka.stream.alpakka.solr.javadsl.SolrSource$).]


Scala
: @@snip [snip](/solr/src/test/scala/akka/stream/alpakka/solr/SolrSpec.scala) { #define-source }

Java
: @@snip [snip](/solr/src/test/java/akka/stream/alpakka/solr/SolrTest.java) { #define-source }

## Sink Usage

Now we can stream messages to Solr by providing the `SolrClient` to the
@scala[@scaladoc[SolrSink](akka.stream.alpakka.solr.scaladsl.SolrSink$).]
@java[@scaladoc[SolrSink](akka.stream.alpakka.solr.javadsl.SolrSink$).]


Scala
: @@snip [snip](/solr/src/test/scala/akka/stream/alpakka/solr/SolrSpec.scala) { #define-class }

Java
: @@snip [snip](/solr/src/test/java/akka/stream/alpakka/solr/SolrTest.java) { #define-class }

### With document sink

Use `SolrSink.document` to stream `SolrInputDocument` to Solr.


Scala
: @@snip [snip](/solr/src/test/scala/akka/stream/alpakka/solr/SolrSpec.scala) { #run-document }

Java
: @@snip [snip](/solr/src/test/java/akka/stream/alpakka/solr/SolrTest.java) { #run-document }

### With bean sink

Firstly, create a POJO.

Scala
: @@snip [snip](/solr/src/test/scala/akka/stream/alpakka/solr/SolrSpec.scala) { #define-bean }

Java
: @@snip [snip](/solr/src/test/java/akka/stream/alpakka/solr/SolrTest.java) { #define-bean }

Use `SolrSink.bean` to stream POJOs to Solr.


Scala
: @@snip [snip](/solr/src/test/scala/akka/stream/alpakka/solr/SolrSpec.scala) { #run-bean }

Java
: @@snip [snip](/solr/src/test/java/akka/stream/alpakka/solr/SolrTest.java) { #run-bean }

### With typed sink

Use `SolrSink.typed` to stream messages with custom binding to Solr.


Scala
: @@snip [snip](/solr/src/test/scala/akka/stream/alpakka/solr/SolrSpec.scala) { #run-typed }

Java
: @@snip [snip](/solr/src/test/java/akka/stream/alpakka/solr/SolrTest.java) { #run-typed }

### Configuration

We can configure the sink by `SolrUpdateSettings`.


Scala
: @@snip [snip](/solr/src/test/scala/akka/stream/alpakka/solr/SolrSpec.scala) { #solr-update-settings }

Java
: @@snip [snip](/solr/src/test/java/akka/stream/alpakka/solr/SolrTest.java) { #solr-update-settings }


| Parameter           | Default | Description                                                                                            |
| ------------------- | ------- | ------------------------------------------------------------------------------------------------------ | 
| commitWithin        | -1      | Max time (in ms) before a commit will happen, -1 for manual committing |

### Update atomically documents

We can update atomically documents.

Scala
: @@snip [snip](/solr/src/test/scala/akka/stream/alpakka/solr/SolrSpec.scala) { #update-atomically-documents }

Java
: @@snip [snip](/solr/src/test/java/akka/stream/alpakka/solr/SolrTest.java) { #update-atomically-documents }

We can use typed and bean to update atomically.

If a collection contains a router field, we have to use the IncomingAtomicUpdateMessage with the router field parameter.

### Delete documents by ids

We can delete documents by ids.

Scala
: @@snip [snip](/solr/src/test/scala/akka/stream/alpakka/solr/SolrSpec.scala) { #delete-documents }

Java
: @@snip [snip](/solr/src/test/java/akka/stream/alpakka/solr/SolrTest.java) { #delete-documents }

We can use typed and bean to delete.

### Delete documents by query

We can delete documents by query.

Scala
: @@snip [snip](/solr/src/test/scala/akka/stream/alpakka/solr/SolrSpec.scala) { #delete-documents-query }

Java
: @@snip [snip](/solr/src/test/java/akka/stream/alpakka/solr/SolrTest.java) { #delete-documents-query }

We can use typed and bean to delete.



## Flow Usage

You can also build flow stages with
@scala[@scaladoc[SolrFlow](akka.stream.alpakka.solr.scaladsl.SolrFlow$).]
@java[@scaladoc[SolrFlow](akka.stream.alpakka.solr.javadsl.SolrFlow$).]
The API is similar to creating Sinks.

Scala
: @@snip [snip](/solr/src/test/scala/akka/stream/alpakka/solr/SolrSpec.scala) { #run-flow }

Java
: @@snip [snip](/solr/src/test/java/akka/stream/alpakka/solr/SolrTest.java) { #run-flow }

### Passing data through SolrFlow

Use `SolrFlow.documentWithPassThrough`, `SolrFlow.beanWithPassThrough` or `SolrFlow.typedWithPassThrough`.

When streaming documents from Kafka, you might want to commit to Kafka **AFTER** the document has been written to Solr.

Scala
: @@snip [snip](/solr/src/test/scala/akka/stream/alpakka/solr/SolrSpec.scala) { #kafka-example }

Java
: @@snip [snip](/solr/src/test/java/akka/stream/alpakka/solr/SolrTest.java) { #kafka-example }


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
