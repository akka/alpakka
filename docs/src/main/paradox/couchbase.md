#Couchbase

The Couchbase connector allows you to read and write to Couchbase. You can query a bucket from CouchbaseSource using N1QL queries or use bulk operation to insert or update with CouchbaseFlow or CouchbaseSink. Couchbase connector uses Couchbase Java SDK behind the scene. 

You can read more about Couchbase [here](https://www.couchbase.com/products/server) and you can read more about Couchbase Java SDK [here](https://developer.couchbase.com/documentation/server/current/sdk/java/start-using-sdk.html)

Couchbase connector supports all Document formats which are supported by SDK. All those formats extends `Document<T>` interface and this is the level of abstraction that connector is using.

## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-couchbase_$scalaBinaryVersion$
  version=$version$
}

#Usage

We will need an @scaladoc[ActorSystem](akka.actor.ActorSystem) and an @scaladoc[ActorMaterializer](akka.stream.ActorMaterializer).

Scala
: @@snip (/couchbase/src/test/scala/docs/scaladsl/CouchbaseSupport.scala) { #init-actor-system }


Then we will need to connect to Couchbase cluster

Scala
: @@snip (/couchbase/src/test/scala/docs/scaladsl/CouchbaseSupport.scala) { #init-cluster }

#Source Usage

We can create different types of `CouchbaseSource`.

From Single Id

Scala
: @@snip (/couchbase/src/test/scala/docs/scaladsl/CouchbaseSupport.scala) { #init-sourceSingle }

From Bulk of Ids

Scala
: @@snip (/couchbase/src/test/scala/docs/scaladsl/CouchbaseSupport.scala) { #init-sourceBulk }

From N1QL query

Scala
: @@snip (/couchbase/src/test/scala/docs/scaladsl/CouchbaseSupport.scala) { #init-sourcen1ql }

#Sink Usage

We can create Sink that upserts single document that extends  `Document<T>`

Scala
: @@snip (/couchbase/src/test/scala/docs/scaladsl/CouchbaseSupport.scala) { #init-SingleSink }

There also an option to upsert bulk of Documents.

Scala
: @@snip (/couchbase/src/test/scala/docs/scaladsl/CouchbaseSupport.scala) { #init-BulkSink }

We can create `CouchbaseSink` for single delete operation

Scala
: @@snip (/couchbase/src/test/scala/docs/scaladsl/CouchbaseSupport.scala) { #delete-SingleSink }

There is also an option to create Sink that deletes bulk of documents by their Ids

Scala
: @@snip (/couchbase/src/test/scala/docs/scaladsl/CouchbaseSupport.scala) { #delete-BulkSink }

#Flow Usage

Couchbase Flow implements exactly the same method as CouchbaseSink, but it unlike Sink, The output of the flow is the following:

- For an operation on single object input will be single Input T and Output will be tuple   of (T and Success or Failure of operation itself).

- For an operation on bulk the input will be Collection of T and output will be the same Collection and Collection of Failed operations object Id and exception.

This is vivid if you want to handle partial or single failures and do not want to affect stream materialization.  

Creating flow with single operation

Scala
: @@snip (/couchbase/src/test/scala/docs/scaladsl/CouchbaseSupport.scala) { #init-SingleFlow }

Creating flow for Bulk operation

Scala
: @@snip (/couchbase/src/test/scala/docs/scaladsl/CouchbaseSupport.scala) { #delete-BulkFlow }

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