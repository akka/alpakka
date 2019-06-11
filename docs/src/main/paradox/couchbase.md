# Couchbase

@@@ note { title="Couchbase"}

Couchbase is an open-source, distributed (shared-nothing architecture) multi-model NoSQL document-oriented database software package that is optimized for interactive applications. These applications may serve many concurrent users by creating, storing, retrieving, aggregating, manipulating and presenting data. In support of these kinds of application needs, Couchbase Server is designed to provide easy-to-scale key-value or JSON document access with low latency and high sustained throughput. It is designed to be clustered from a single machine to very large-scale deployments spanning many machines. 

Couchbase provides client protocol compatibility with memcached, but adds disk persistence, data replication, live cluster reconfiguration, rebalancing and multitenancy with data partitioning. 

-- [Wikipedia](https://en.wikipedia.org/wiki/Couchbase_Server)

@@@

Alpakka Couchbase allows you to read and write to Couchbase. You can query a bucket from CouchbaseSource using N1QL queries or reading by document ID. Couchbase connector uses @extref[Couchbase Java SDK](couchbase:start-using-sdk.html) version @var[couchbase.version] behind the scenes.

The Couchbase connector supports all document formats which are supported by the SDK. All those formats use the @java[`Document<T>`]@scala[`Document[T]`] interface and this is the level of abstraction that this connector is using.


@@project-info{ projectId="couchbase" }


## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-couchbase_$scala.binary.version$
  version=$project.version$
}

The table below shows direct dependencies of this module and the second tab shows all libraries it depends on transitively.

@@dependencies { projectId="couchbase" }

# Overview

Alpakka Couchbase offers both @ref:[Akka Streams APIs](#reading-from-couchbase-in-akka-streams) and a more @ref:[direct API](#using-couchbasesession-directly) to access Couchbase:

* `CouchbaseSession` (@scala[@scaladoc[API](akka.stream.alpakka.couchbase.scaladsl.CouchbaseSession)]@java[@scaladoc[API](akka.stream.alpakka.couchbase.javadsl.CouchbaseSession)]) offers a direct API for one-off operations
* `CouchbaseSessionRegistry` (@scaladoc[API](akka.stream.alpakka.couchbase.CouchbaseSessionRegistry$)) is an Akka extension to keep track and share `CouchbaseSession`s within an `ActorSystem`
* `CouchbaseSource` (@scala[@scaladoc[API](akka.stream.alpakka.couchbase.scaladsl.CouchbaseSource$)]@java[@scaladoc[API](akka.stream.alpakka.couchbase.javadsl.CouchbaseSource$)]), `CouchbaseFlow` (@scala[@scaladoc[API](akka.stream.alpakka.couchbase.scaladsl.CouchbaseFlow$)]@java[@scaladoc[API](akka.stream.alpakka.couchbase.javadsl.CouchbaseFlow$)]), and `CouchbaseSink` (@scala[@scaladoc[API](akka.stream.alpakka.couchbase.scaladsl.CouchbaseSink$)]@java[@scaladoc[API](akka.stream.alpakka.couchbase.javadsl.CouchbaseSink$)]) offer factory methods to create Akka Stream operators

## Configuration

All operations use the `CouchbaseSession` internally. A session is configured with `CouchbaseSessionSettings` (@scaladoc[API](akka.stream.alpakka.couchbase.CouchbaseSessionSettings$)) and a Couchbase bucket name. The Akka Stream factory methods create and access the corresponding session instance behind the scenes.

By default the `CouchbaseSessionSettings` are read from the `alpakka.couchbase.session` section from the configuration eg. in your `application.conf`.

Settings
: @@snip [snip](/couchbase/src/test/resources/application.conf) { #settings }

## Using Akka Discovery

To delegate the configuration of Couchbase nodes to any of [Akka Discovery's lookup mechanisms](https://doc.akka.io/docs/akka/current/discovery/index.html), specify a service name and lookup timeout in the Couchbase section, and pass in @scala[@scaladoc[DiscoverySupport](akka.stream.alpakka.couchbase.scaladsl.DiscoverySupport$)]@java[@scaladoc[DiscoverySupport](akka.stream.alpakka.couchbase.javadsl.DiscoverySupport)] nodes lookup to `enrichAsync` and configure Akka Discovery accordingly.

**The Akka Discovery dependency has to be added explicitly**.

Discovery settings (Config discovery)
: @@snip [snip](/couchbase/src/test/resources/discovery.conf) { #discovery-settings }


To enable Akka Discovery on the `CouchbaseSessionSettings`, use `DiscoverySupport.nodes()` as enrichment function.

Scala
: @@snip [snip](/couchbase/src/test/scala/docs/scaladsl/DiscoverySpec.scala) { #registry }

Java
: @@snip [snip](/couchbase/src/test/java/docs/javadsl/DiscoveryTest.java) { #registry }


# Reading from Couchbase in Akka Streams

## Using statements

To query Couchbase using the statement DSL use `CouchbaseSource.fromStatement`. 

Scala
: @@snip [snip](/couchbase/src/test/scala/docs/scaladsl/CouchbaseSourceSpec.scala) { #statement }

Java
: @@snip [snip](/couchbase/src/test/java/docs/javadsl/CouchbaseExamplesTest.java) { #statement }


## Using N1QL queries

To query Couchbase using the @extref["N1QL" queries](couchbase:n1ql-query.html) use `CouchbaseSource.fromN1qlQuery`. 

Scala
: @@snip [snip](/couchbase/src/test/scala/docs/scaladsl/CouchbaseSourceSpec.scala) { #n1ql }

Java
: @@snip [snip](/couchbase/src/test/java/docs/javadsl/CouchbaseExamplesTest.java) { #n1ql }


## Get by ID

`CouchbaseFlow.fromId` methods allow to read documents specified by the document ID in the Akka Stream.

Scala
: @@snip [snip](/couchbase/src/test/scala/docs/scaladsl/CouchbaseFlowSpec.scala) { #fromId }

Java
: @@snip [snip](/couchbase/src/test/java/docs/javadsl/CouchbaseExamplesTest.java) { #fromId }


# Writing to Couchbase in Akka Streams

For each mutation operation we need to create `CouchbaseWriteSettings` instance which consists of the following parameters

- Parallelism in access to Couchbase (default 1)
- Couchbase Replication Factor (default `ReplicateTo.NONE`) 
- Couchbase Persistence Level for Write Operation (default `PersistTo.NONE`)
- 2 seconds operation timeout 

These default values are not recommended for production use, as they do not persist to disk for any node. 

Scala
: @@snip [snip](/couchbase/src/test/scala/docs/scaladsl/CouchbaseFlowSpec.scala) { #write-settings }

Java
: @@snip [snip](/couchbase/src/test/java/docs/javadsl/CouchbaseExamplesTest.java) { #write-settings }


Read more about durability settings in the @extref[Couchbase documentation](couchbase:durability.html#configuring-durability). 

## Upsert

The `CouchbaseFlow` and `CouchbaseSink` offer factories for upserting documents (insert or update) in Couchbase. `upsert` is used for the most commonly used `JsonDocument`, and `upsertDoc` has as type parameter to support any variants of @scala[`Document[T]`]@java[`Document<T>`] which may be `RawJsonDocument`, `StringDocument` or `BinaryDocument`.

The `upsert` and `upsertDoc` operators fail the stream on any error when writing to Couchbase. To handle failures in-stream use `upsertDocWithResult` shown below. 

Scala
: @@snip [snip](/couchbase/src/test/scala/docs/scaladsl/CouchbaseFlowSpec.scala) { #upsert }

Java
: @@snip [snip](/couchbase/src/test/java/docs/javadsl/CouchbaseExamplesTest.java) { #upsert }


@@@ note

For single document modifications you may consider using the `CouchbaseSession` methods directly, they offer a @scala[future-based]@java[CompletionStage-based] API which in many cases might be simpler than using Akka Streams with just one element (see [below](#using-couchbasesession-directly))

@@@

Couchbase writes may fail temporarily for a particular node. If you want to handle such failures without restarting the whole stream, the `upsertDocWithResult` operator captures failures from Couchbase and emits `CouchbaseWriteResult` sub-classes `CouchbaseWriteSuccess` and `CouchbaseWriteFailure` downstream.

Scala
: @@snip [snip](/couchbase/src/test/scala/docs/scaladsl/CouchbaseFlowSpec.scala) { #upsertDocWithResult }

Java
: @@snip [snip](/couchbase/src/test/java/docs/javadsl/CouchbaseExamplesTest.java) { #upsertDocWithResult }


## Delete

The `CouchbaseFlow` and `CouchbaseSink` offer factories to delete documents in Couchbase by ID.

Scala
: @@snip [snip](/couchbase/src/test/scala/docs/scaladsl/CouchbaseFlowSpec.scala) { #delete }

Java
: @@snip [snip](/couchbase/src/test/java/docs/javadsl/CouchbaseExamplesTest.java) { #delete }

To handle any delete failures such as non-existent documents in-stream, use the the `deleteWithResult` operator. It captures failures from Couchbase and emits `CouchbaseDeleteResult`s.

Scala
: @@snip [snip](/couchbase/src/test/scala/docs/scaladsl/CouchbaseFlowSpec.scala) { #deleteWithResult }

Java
: @@snip [snip](/couchbase/src/test/java/docs/javadsl/CouchbaseExamplesTest.java) { #deleteWithResult }


# Using `CouchbaseSession` directly

## Access via registry

The `CouchbaseSesionRegistry` is an Akka extension to manage the life-cycle of Couchbase sessions. All underlying instances are closed upon actor system termination.

When accessing more than one Couchbase cluster, the `CouchbaseEnvironment` should be shared by setting a single instance for the different `CouchbaseSessionSettings`.

Scala
: @@snip [snip](/couchbase/src/test/scala/docs/scaladsl/CouchbaseSessionExamplesSpec.scala) { #registry }

Java
: @@snip [snip](/couchbase/src/test/java/docs/javadsl/CouchbaseExamplesTest.java) { #registry }


## Manage session life-cycle
Use `CouchbaseSessionSettings` to get an instance of `CouchbaseSession`. These settings may be specified in `application.conf` and complemented in code. Furthermore a session requires the bucket name and needs an `ExecutionContext` as the creation is asynchronous.

Scala
: @@snip [snip](/couchbase/src/test/scala/docs/scaladsl/CouchbaseSessionExamplesSpec.scala) { #create }

Java
: @@snip [snip](/couchbase/src/test/java/docs/javadsl/CouchbaseExamplesTest.java) { #session }


## Manage bucket life-cycle

For full control a `CouchbaseSession` may be created from a Couchbase `Bucket`. See @extref:[Scalability and Concurrency](couchbase:managing-connections.html#concurrency) in the Couchbase documentation for details.

Scala
: @@snip [snip](/couchbase/src/test/scala/docs/scaladsl/CouchbaseSessionExamplesSpec.scala) { #fromBucket }

Java
: @@snip [snip](/couchbase/src/test/java/docs/javadsl/CouchbaseExamplesTest.java) { #sessionFromBucket }

To learn about the full range of operations on `CouchbaseSession`, read the @scala[@scaladoc[API docs](akka.stream.alpakka.couchbase.scaladsl.CouchbaseSession)]@java[@scaladoc[API docs](akka.stream.alpakka.couchbase.javadsl.CouchbaseSession)].
