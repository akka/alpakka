#Couchbase

The Couchbase connector allows you to read and write to Couchbase. You can query a bucket from CouchbaseSource using N1QL queries or use bulk operation to insert or update with CouchbaseFlow or CouchbaseSink. Couchbase connector uses Couchbase Java SDK behind the scene. 

You can read more about Couchbase [here](https://www.couchbase.com/products/server) and you can read more about Couchbase Java SDK [here](https://developer.couchbase.com/documentation/server/current/sdk/java/start-using-sdk.html)

The Couchbase connector supports all document formats which are supported by SDK. All those formats extend `Document<T>` interface and this is the level of abstraction that connector is using.

@@project-info{ projectId="couchbase" }

## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-couchbase_$scalaBinaryVersion$
  version=$version$
}

The table below shows direct dependencies of this module and the second tab shows all libraries it depends on transitively.

@@dependencies { projectId="couchbase" }


#Usage

We will need an @scaladoc[ActorSystem](akka.actor.ActorSystem) and an @scaladoc[ActorMaterializer](akka.stream.ActorMaterializer).

Scala
: @@snip (/couchbase/src/test/scala/docs/scaladsl/CouchbaseSupport.scala) { #init-actor-system }

Java
: @@snip (/couchbase/src/test/java/docs/javadsl/Examples.java) { #init-actor-system }



Then we will need to connect to Couchbase cluster

Scala
: @@snip (/couchbase/src/test/scala/docs/scaladsl/CouchbaseSourceSpec.scala) { #init-cluster }

Java
: @@snip (/couchbase/src/test/java/docs/javadsl/Examples.java) { #cluster-connect }

#Source Usage

We can create different types of `CouchbaseSource`.

From Single Id

Scala
: @@snip (/couchbase/src/test/scala/docs/scaladsl/CouchbaseFlowSpec.scala) { #init-sourceSingle }

Java
: @@snip (/couchbase/src/test/java/docs/javadsl/Examples.java) { #init-sourceSingle }
 
From Bulk of Ids

Scala
: @@snip (/couchbase/src/test/scala/docs/scaladsl/CouchbaseFlowSpec.scala) { #init-sourceBulk }

Java
: @@snip (/couchbase/src/test/java/docs/javadsl/Examples.java) { #init-sourceBulk }
        

From N1QL query

Scala
: @@snip (/couchbase/src/test/scala/docs/scaladsl/CouchbaseSourceSpec.scala) { #init-sourcen1ql }

Java
: @@snip (/couchbase/src/test/java/docs/javadsl/Examples.java) { #init-sourcen1ql }

#Write Operation Settings

For each mutation operation we need to create `CouchbaseWriteSettings` instance which consists of the following parameters

- Stage Parallelism (default 1)
- Couchbase Replication Factor (default `ReplicateTo.ONE`) 
- Couchbase Persistance Level for Write Operation (default `PersistTo.NONE`)
- 2 seconds operation timeout 

Last two default values mean that we will observe replication to one replica znd do not observe persistence on disk for any node,

Scala
: @@snip (/couchbase/src/test/scala/docs/scaladsl/CouchbaseFlowSpec.scala) { #write-settings }

Java
: @@snip (/couchbase/src/test/java/docs/javadsl/Examples.java) { #write-settings }


You can read more about it [here](https://docs.couchbase.com/java-sdk/2.6/durability.html#configuring-durability) 

#Sink Usage


We can create Sink that upserts single document that extends  `Document<T>`

Scala
: @@snip (/couchbase/src/test/scala/docs/scaladsl/CouchbaseSupport.scala) { #init-SingleSink }

Java
: @@snip (/couchbase/src/test/java/docs/javadsl/Examples.java) { #upsertSingle }

There also an option to upsert bulk of Documents.

Scala
: @@snip (/couchbase/src/test/scala/docs/scaladsl/CouchbaseSupport.scala) { #init-BulkSink }

Java
: @@snip (/couchbase/src/test/java/docs/javadsl/Examples.java) { #upsertBulk }

We can create `CouchbaseSink` for single delete operation

Scala
: @@snip (/couchbase/src/test/scala/docs/scaladsl/CouchbaseSupport.scala) { #delete-SingleSink }

Java
: @@snip (/couchbase/src/test/java/docs/javadsl/Examples.java) { #delete-single-sink }

There is also an option to create Sink that deletes bulk of documents by their Ids

Scala
: @@snip (/couchbase/src/test/scala/docs/scaladsl/CouchbaseSupport.scala) { #delete-BulkSink }

Java
: @@snip (/couchbase/src/test/java/docs/javadsl/Examples.java) { #delete-bulk-sink }

#Flow Usage

Couchbase Flow implements exactly the same method for upsert and delete as `CouchbaseSink`, but unlike Sink, an output of the Flow Stage is the following:

- For an operation on single object output will be instance of `SingleOperationResult` class which consists of input `T` and optional exception of failed operation 

- For an operation on bulk the output will be instance of `BulkOperationResult` class which consists of Collection of input elements and Collection failed documents Id's and its relevant Exception. 

This is vivid if you want to handle partial or single failures and do not want to affect stream materialization.  

Creating flow with single upsert operation

Scala
: @@snip (/couchbase/src/test/scala/docs/scaladsl/CouchbaseSupport.scala) { #init-SingleFlow }

Java
: @@snip (/couchbase/src/test/java/docs/javadsl/Examples.java) { #upsertFlowSingle }

Creating flow for delete Bulk operation

Scala
: @@snip (/couchbase/src/test/scala/docs/scaladsl/CouchbaseSupport.scala) { #delete-BulkFlow }

Java
: @@snip (/couchbase/src/test/java/docs/javadsl/Examples.java) { #delete-bulk-flow }

You can create Flow stage which has single Id as input and Couchbase Document as output.

Scala
: @@snip (/couchbase/src/test/scala/docs/scaladsl/CouchbaseFlowSpec.scala) { #by-single-id-flow }

Java
: @@snip (/couchbase/src/test/java/docs/javadsl/Examples.java) { #by-single-id-flow } 

There is also a possibility to create Flow from bulk of Ids

Scala
: @@snip (/couchbase/src/test/scala/docs/scaladsl/CouchbaseSourceSpec.scala) { #by-bulk-id-flow }

###Dealing with Consistency Level and Durability.

For every mutation method in Sink and Flow there is an option to set `PersistTo` and `ReplicateTo` parameters. You can read more about it [here](https://docs.couchbase.com/java-sdk/2.6/durability.html#configuring-durability)

### Running the example code

The code in this guide is part of runnable tests of this project. You are welcome to edit the code and run it in sbt.

> Test code requires a Couchbase server running in the background. You can start one quickly using docker:
>
> `docker-compose up couchbase`

Scala
:   ```
    sbt
    > couchbase/test
    ```
