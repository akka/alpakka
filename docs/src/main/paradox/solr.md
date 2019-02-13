# Apache Solr

@@@ note { title="Apache Solr" }

Solr (pronounced "solar") is an open source enterprise search platform, written in Java, from the Apache Lucene project. Its major features include full-text search, hit highlighting, faceted search, real-time indexing, dynamic clustering, database integration, NoSQL features and rich document (e.g., Word, PDF) handling. Providing distributed search and index replication, Solr is designed for scalability and fault tolerance. Solr is widely used for enterprise search and analytics use cases and has an active development community and regular releases.

-- [Wikipedia](https://en.wikipedia.org/wiki/Apache_Solr)
 
@@@

Alpakka Solr provides Akka Stream sources and sinks for Apache Solr.

For more information about Solr please visit the [Solr documentation](http://lucene.apache.org/solr/resources.html).

@@project-info{ projectId="solr" }

## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-solr_$scala.binary.version$
  version=$project.version$
}

The table below shows direct dependencies of this module and the second tab shows all libraries it depends on transitively.

@@dependencies { projectId="solr" }


## Set up client

Sources, Flows and Sinks provided by this connector need a prepared `org.apache.solr.client.solrj.SolrClient` (eg. `org.apache.solr.client.solrj.impl.CloudSolrClient`) to access to Solr.


Scala
: @@snip [snip](/solr/src/test/scala/docs/scaladsl/SolrSpec.scala) { #init-client }

Java
: @@snip [snip](/solr/src/test/java/docs/javadsl/SolrTest.java) { #init-client }


## Reading from Solr

Create a tuple stream and use `SolrSource.fromTupleStream` (@scala[@scaladoc[API](akka.stream.alpakka.solr.scaladsl.SolrSource$)]@java[@scaladoc[API](akka.stream.alpakka.solr.javadsl.SolrSource$)]) to create a source.

Scala
: @@snip [snip](/solr/src/test/scala/docs/scaladsl/SolrSpec.scala) { #tuple-stream }

Java
: @@snip [snip](/solr/src/test/java/docs/javadsl/SolrTest.java) { #tuple-stream }


## Writing to Solr

Alpakka Solr batches updates to Solr by sending all updates of the same operation at once to Solr. These batches are extracted from the elements within one collection sent to a Solr flow or sink. Updates of different types may be contained in a single collection sent, though. In case streams don't have natural batches of updates, you may use the `groupedWithin` operator to create count or time-based batches.

## Configuration for updates

Data sent to Solr is not searchable until it has been committed to the index. These are the major options for handling commits:

1. The Solr installation can be configured to use **auto-commit**.
2. Specify **commit-within** in `SolrUpdateSettings` to trigger commits after every write through Alpakka Solr.
3. Use explicit committing via the `SolrClient.commit` methods on stream completion as most examples show. As `commit` is a blocking operation, choose an appropriate execution context (preferably not `system.dispatcher`).

Configuration of Solr committing is described in [UpdateHandlers in SolrConfig](http://lucene.apache.org/solr/guide/7_6/updatehandlers-in-solrconfig.html#commits).

Scala
: @@snip [snip](/solr/src/test/scala/docs/scaladsl/SolrSpec.scala) { #solr-update-settings }

Java
: @@snip [snip](/solr/src/test/java/docs/javadsl/SolrTest.java) { #solr-update-settings }


| Parameter           | Default | Description                                                                                            |
| ------------------- | ------- | ------------------------------------------------------------------------------------------------------ | 
| commitWithin        | -1      | Max time (in ms) before a commit will happen, -1 for manual committing |



Now we can stream messages to Solr by providing the `SolrClient` to the
@scala[@scaladoc[SolrSink](akka.stream.alpakka.solr.scaladsl.SolrSink$).]
@java[@scaladoc[SolrSink](akka.stream.alpakka.solr.javadsl.SolrSink$).]


Scala
: @@snip [snip](/solr/src/test/scala/docs/scaladsl/SolrSpec.scala) { #define-class }

Java
: @@snip [snip](/solr/src/test/java/docs/javadsl/SolrTest.java) { #define-class }

### With document sink

Use `SolrSink.document` to stream `SolrInputDocument` to Solr.


Scala
: @@snip [snip](/solr/src/test/scala/docs/scaladsl/SolrSpec.scala) { #run-document }

Java
: @@snip [snip](/solr/src/test/java/docs/javadsl/SolrTest.java) { #run-document }

### With bean sink

Firstly, create a POJO.

Scala
: @@snip [snip](/solr/src/test/scala/docs/scaladsl/SolrSpec.scala) { #define-bean }

Java
: @@snip [snip](/solr/src/test/java/docs/javadsl/SolrTest.java) { #define-bean }

Use `SolrSink.bean` to stream POJOs to Solr.


Scala
: @@snip [snip](/solr/src/test/scala/docs/scaladsl/SolrSpec.scala) { #run-bean }

Java
: @@snip [snip](/solr/src/test/java/docs/javadsl/SolrTest.java) { #run-bean }

### With typed sink

Use `SolrSink.typed` to stream messages with custom binding to Solr.


Scala
: @@snip [snip](/solr/src/test/scala/docs/scaladsl/SolrSpec.scala) { #run-typed }

Java
: @@snip [snip](/solr/src/test/java/docs/javadsl/SolrTest.java) { #run-typed }


### Update atomically documents

We can update atomically documents.

Scala
: @@snip [snip](/solr/src/test/scala/docs/scaladsl/SolrSpec.scala) { #update-atomically-documents }

Java
: @@snip [snip](/solr/src/test/java/docs/javadsl/SolrTest.java) { #update-atomically-documents }

We can use typed and bean to update atomically.

If a collection contains a router field, we have to use the IncomingAtomicUpdateMessage with the router field parameter.

### Delete documents by ids

We can delete documents by ids.

Scala
: @@snip [snip](/solr/src/test/scala/docs/scaladsl/SolrSpec.scala) { #delete-documents }

Java
: @@snip [snip](/solr/src/test/java/docs/javadsl/SolrTest.java) { #delete-documents }

We can use typed and bean to delete.

### Delete documents by query

We can delete documents by query.

Scala
: @@snip [snip](/solr/src/test/scala/docs/scaladsl/SolrSpec.scala) { #delete-documents-query }

Java
: @@snip [snip](/solr/src/test/java/docs/javadsl/SolrTest.java) { #delete-documents-query }

We can use typed and bean to delete.



## Flow Usage

You can also build flow stages with
@scala[@scaladoc[SolrFlow](akka.stream.alpakka.solr.scaladsl.SolrFlow$).]
@java[@scaladoc[SolrFlow](akka.stream.alpakka.solr.javadsl.SolrFlow$).]
The API is similar to creating Sinks.

Scala
: @@snip [snip](/solr/src/test/scala/docs/scaladsl/SolrSpec.scala) { #run-flow }

Java
: @@snip [snip](/solr/src/test/java/docs/javadsl/SolrTest.java) { #run-flow }

### Passing data through SolrFlow

Use `SolrFlow.documentWithPassThrough`, `SolrFlow.beanWithPassThrough` or `SolrFlow.typedWithPassThrough`.

When streaming documents from Kafka, you might want to commit to Kafka **AFTER** the document has been written to Solr.

Scala
: @@snip [snip](/solr/src/test/scala/docs/scaladsl/SolrSpec.scala) { #kafka-example }

Java
: @@snip [snip](/solr/src/test/java/docs/javadsl/SolrTest.java) { #kafka-example }


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
