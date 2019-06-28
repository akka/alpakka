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


## Set up a Solr client

Sources, Flows and Sinks provided by this connector need a prepared @javadoc[`SolrClient`](org.apache.solr.client.solrj.SolrClient) (eg. @javadoc[`CloudSolrClient`](org.apache.solr.client.solrj.impl.CloudSolrClient)) to access to Solr.


Scala
: @@snip [snip](/solr/src/test/scala/docs/scaladsl/SolrSpec.scala) { #init-client }

Java
: @@snip [snip](/solr/src/test/java/docs/javadsl/SolrTest.java) { #init-client }


## Reading from Solr

Create a @javadoc[Solr `TupleStream`](org.apache.solr.client.solrj.io.stream.TupleStream) (eg. via @javadoc[`CloudSolrStream`](org.apache.solr.client.solrj.io.stream.CloudSolrStream)) and use `SolrSource.fromTupleStream` (@scala[@scaladoc[API](akka.stream.alpakka.solr.scaladsl.SolrSource$)]@java[@scaladoc[API](akka.stream.alpakka.solr.javadsl.SolrSource$)]) to create a source.

Scala
: @@snip [snip](/solr/src/test/scala/docs/scaladsl/SolrSpec.scala) { #tuple-stream }

Java
: @@snip [snip](/solr/src/test/java/docs/javadsl/SolrTest.java) { #tuple-stream }


## Writing to Solr

Alpakka Solr batches updates to Solr by sending all updates of the same operation type at once to Solr. These batches are extracted from the elements within one collection sent to a Solr flow or sink. Updates of different types may be contained in a single collection sent, though. In case streams don't have natural batches of updates, you may use the `groupedWithin` operator to create count or time-based batches.

Alpakka Solr offers three styles for writing to Apache Solr:

1. Using @javadoc[`SolrInputDocument`](org.apache.solr.common.SolrInputDocument) (via `SolrSink.documents`, `SolrFlow.documents` and `SolrFlow.documentsWithPassThrough`)
1. Annotated *Java Bean* classes supported by @javadoc[Solr's `DocumentObjectBinder`](org.apache.solr.client.solrj.beans.DocumentObjectBinder) (via `SolrSink.beans`, `SolrFlow.beans` and `SolrFlow.beansWithPassThrough`)
1. Typed streams with document binders to translate to @javadoc[`SolrInputDocument`](org.apache.solr.common.SolrInputDocument) (via `SolrSink.typeds`, `SolrFlow.typeds` and `SolrFlow.typedsWithPassThrough`)

In all variations the data is wrapped into `WriteMessage`s.


### Committing and configuration for updates

Data sent to Solr is not searchable until it has been committed to the index. These are the major options for handling commits:

1. The Solr installation can be configured to use **auto-commit**.
2. Specify **commit-within** in `SolrUpdateSettings` to trigger commits after every write through Alpakka Solr.
3. Use explicit committing via the `SolrClient.commit` methods on stream completion as most examples show. As `commit` is a blocking operation, choose an appropriate execution context (preferably *not* `system.dispatcher`).

Configuration of Solr committing is described in @extref[UpdateHandlers in SolrConfig](solr:updatehandlers-in-solrconfig.html#commits).


#### Available settings
| Parameter           | Default | Description                                                                                            |
| ------------------- | ------- | ------------------------------------------------------------------------------------------------------ | 
| commitWithin        | -1      | Max time (in ms) before a commit will happen, -1 for explicit committing |


Scala
: @@snip [snip](/solr/src/test/scala/docs/scaladsl/SolrSpec.scala) { #solr-update-settings }

Java
: @@snip [snip](/solr/src/test/java/docs/javadsl/SolrTest.java) { #solr-update-settings }



### Writing `SolrInputDocument`s

Use `SolrSink.document` to stream `SolrInputDocument` to Solr.

#### Defining mappings

Scala
: @@snip [snip](/solr/src/test/scala/docs/scaladsl/SolrSpec.scala) { #define-class }

Java
: @@snip [snip](/solr/src/test/java/docs/javadsl/SolrTest.java) { #define-class }


Use `SolrSink.documents`, `SolrFlow.documents` or `SolrFlow.documentsWithPassThrough` to stream `SolrInputDocument`s to Solr.

A `SolrClient` must be provided to
@scala[@scaladoc[SolrSink](akka.stream.alpakka.solr.scaladsl.SolrSink$) implicitly.]
@java[@scaladoc[SolrSink](akka.stream.alpakka.solr.javadsl.SolrSink$).]

Scala
: @@snip [snip](/solr/src/test/scala/docs/scaladsl/SolrSpec.scala) { #run-document }

Java
: @@snip [snip](/solr/src/test/java/docs/javadsl/SolrTest.java) { #run-document }


### Writing Java beans

Firstly, create a POJO.

Scala
: @@snip [snip](/solr/src/test/scala/docs/scaladsl/SolrSpec.scala) { #define-bean }

Java
: @@snip [snip](/solr/src/test/java/docs/javadsl/SolrTest.java) { #define-bean }

Use `SolrSink.beans`, `SolrFlow.beans` or `SolrFlow.beansWithPassThrough` to stream POJOs to Solr.

Scala
: @@snip [snip](/solr/src/test/scala/docs/scaladsl/SolrSpec.scala) { #run-bean }

Java
: @@snip [snip](/solr/src/test/java/docs/javadsl/SolrTest.java) { #run-bean }


### Writing arbitrary classes via custom binding

Use `SolrSink.typeds`, `SolrFlow.typeds` or `SolrFlow.typedsWithPassThrough` to stream messages with custom binding to Solr.

Scala
: @@snip [snip](/solr/src/test/scala/docs/scaladsl/SolrSpec.scala) { #run-typed }

Java
: @@snip [snip](/solr/src/test/java/docs/javadsl/SolrTest.java) { #run-typed }


#### Using a flow with custom binding

You can also build flow stages with
@scala[@scaladoc[SolrFlow](akka.stream.alpakka.solr.scaladsl.SolrFlow$).]
@java[@scaladoc[SolrFlow](akka.stream.alpakka.solr.javadsl.SolrFlow$).]

Scala
: @@snip [snip](/solr/src/test/scala/docs/scaladsl/SolrSpec.scala) { #typeds-flow }

Java
: @@snip [snip](/solr/src/test/java/docs/javadsl/SolrTest.java) { #typeds-flow }


#### Passing data through SolrFlow

All flow types (`documents`, `beans`, `typeds`) exist with pass-through support:
Use `SolrFlow.documentsWithPassThrough`, `SolrFlow.beansWithPassThrough` or `SolrFlow.typedsWithPassThrough`.

When streaming documents from Kafka, you might want to commit to Kafka **AFTER** the document has been written to Solr. This scenario uses implicit committing via the **commit within** setting.

Scala
: @@snip [snip](/solr/src/test/scala/docs/scaladsl/SolrSpec.scala) { #kafka-example }

Java
: @@snip [snip](/solr/src/test/java/docs/javadsl/SolrTest.java) { #kafka-example }

#### Excluding messages

Failure to deserialize a kafka message is a particular case of conditional message processing.
It is also likely that we would have no message to produce to SolR when we encounter messages that fail to deserialize.
The solr flow will not let us pass through the corresponding committable offset without doing a request to solr.

Use `WriteMessage.createPassThrough` to exclude this message without doing any change on solr inside a flow.

Scala
: @@snip [snip](/solr/src/test/scala/docs/scaladsl/SolrSpec.scala) { #kafka-example-PT }

Java
: @@snip [snip](/solr/src/test/java/docs/javadsl/SolrTest.java) { #kafka-example-PT }


## Update documents

With `WriteMessage.createUpdateMessage` documents can be updated atomically. All flow and sink types (`documents`, `beans`, `typeds`) support atomic updates.

Scala
: @@snip [snip](/solr/src/test/scala/docs/scaladsl/SolrSpec.scala) { #update-atomically-documents }

Java
: @@snip [snip](/solr/src/test/java/docs/javadsl/SolrTest.java) { #update-atomically-documents }

If a collection contains a router field, use the `WriteMessage.createUpdateMessage(...).withRoutingFieldValue(..)` to set the router field.


## Delete documents by ids

With `WriteMessage.createDeleteMessage(id)` documents may be deleted by ID. All flow and sink types (`documents`, `beans`, `typeds`) support deleting.

Scala
: @@snip [snip](/solr/src/test/scala/docs/scaladsl/SolrSpec.scala) { #delete-documents }

Java
: @@snip [snip](/solr/src/test/java/docs/javadsl/SolrTest.java) { #delete-documents }


## Delete documents by query

With `WriteMessage.createDeleteByQueryMessage(query)` documents matching a query may be deleted. All flow and sink types (`documents`, `beans`, `typeds`) support deleting.

Scala
: @@snip [snip](/solr/src/test/scala/docs/scaladsl/SolrSpec.scala) { #delete-documents-query }

Java
: @@snip [snip](/solr/src/test/java/docs/javadsl/SolrTest.java) { #delete-documents-query }
