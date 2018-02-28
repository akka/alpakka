# MongoDB Connector

The MongoDB connector allows you to read and save documents. You can query as a stream of documents from @scaladoc[MongoSource](akka.stream.alpakka.mongodb.scaladsl.MongoSource$) or update documents in a collection with @scaladoc[MongoSink](akka.stream.alpakka.mongodb.scaladsl.MongoSink$).

This connector is based off the [mongo-scala-driver](https://github.com/mongodb/mongo-scala-driver) and does not have a java interface. It supports driver macros and codec allowing to read or write scala case class objects.


### Reported issues

[Tagged issues at Github](https://github.com/akka/alpakka/labels/p%3Amongodb)


## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-mongodb_$scalaBinaryVersion$
  version=$version$
}

## Usage

Sources provided by this connector need a prepared session to communicate with MongoDB server.

For codec and macros support, you first need to provide a case class and a codecRegistry.

Scala
: @@snip ($alpakka$/mongodb/src/test/scala/akka/stream/alpakka/mongodb/MongoSourceSpec.scala) { #macros-codecs }

Then, lets initialize a MongoDB connection.

Scala
: @@snip ($alpakka$/mongodb/src/test/scala/akka/stream/alpakka/mongodb/MongoSourceSpec.scala) { #init-connection }

For codec support, add the registry to the database or the collection.

Scala
: @@snip ($alpakka$/mongodb/src/test/scala/akka/stream/alpakka/mongodb/MongoSourceSpec.scala) { #init-connection-codec }

We will also need an @scaladoc[ActorSystem](akka.actor.ActorSystem) and an @scaladoc[ActorMaterializer](akka.stream.ActorMaterializer).

Scala
: @@snip ($alpakka$/mongodb/src/test/scala/akka/stream/alpakka/mongodb/MongoSourceSpec.scala) { #init-mat }

This is all preparation that we are going to need.

### Source Usage

Let's create a source from a MongoDB collection observable, which can optionally take a filter.

Scala
: @@snip ($alpakka$/mongodb/src/test/scala/akka/stream/alpakka/mongodb/MongoSourceSpec.scala) { #create-source }

With codec support, adapt the type of the source.

Scala
: @@snip ($alpakka$/mongodb/src/test/scala/akka/stream/alpakka/mongodb/MongoSourceSpec.scala) { #create-source-codec }

And finally we can run it.

Scala
: @@snip ($alpakka$/mongodb/src/test/scala/akka/stream/alpakka/mongodb/MongoSourceSpec.scala) { #run-source }

With codec support

Scala
: @@snip ($alpakka$/mongodb/src/test/scala/akka/stream/alpakka/mongodb/MongoSourceSpec.scala) { #run-source-codec }

Here we used a basic sink to complete the stream by collecting all of the stream elements to a collection. The power of streams comes from building larger data pipelines which leverage backpressure to ensure efficient flow control. Feel free to edit the example code and build @extref[more advanced stream topologies](akka-docs:scala/stream/stream-introduction).

### Flow and Sink Usage

Each of these sink factory methods have a corresponding factory in @scaladoc[insertOne](akka.stream.alpakka.mongodb.scaladsl.MongoFlow) which will emit the written document or result of the operation downstream.

For codec support, the type must be specified in the database or collection declaration.

Scala
: @@snip ($alpakka$/mongodb/src/test/scala/akka/stream/alpakka/mongodb/MongoSinkSpec.scala) { #init-connection-codec }

#### Insert

We can use a Source of documents to save them to a mongo collection using @scaladoc[insertOne](akka.stream.alpakka.mongodb.scaladsl.MongoSink$#insertOne) or @scaladoc[insertMany](akka.stream.alpakka.mongodb.scaladsl.MongoSink$#insertMany).


Scala
: @@snip (../../../../mongodb/src/test/scala/akka/stream/alpakka/mongodb/MongoSinkSpec.scala) { #insertOne }

With codec support

Scala
: @@snip ($alpakka$/mongodb/src/test/scala/akka/stream/alpakka/mongodb/MongoSinkSpec.scala) { #insertOneCodec }

#### Insert Many

Insert many can be used if you have a collection of documents to insert at once.

Scala
: @@snip (../../../../mongodb/src/test/scala/akka/stream/alpakka/mongodb/MongoSinkSpec.scala) { #insertMany }

With codec support

Scala
: @@snip ($alpakka$/mongodb/src/test/scala/akka/stream/alpakka/mongodb/MongoSinkSpec.scala) { #insertManyCodec }

#### Update

We can update documents with a Source of @scaladoc[DocumentUpdate](akka.stream.alpakka.mongodb.scaladsl.DocumentUpdate) which is a filter and a update definition.
Use either @scaladoc[updateOne](akka.stream.alpakka.mongodb.scaladsl.MongoSink$#updateOne) or @scaladoc[updateMany](akka.stream.alpakka.mongodb.scaladsl.MongoSink$#updateMany) if the filter should target one or many documents.

Scala
: @@snip (../../../../mongodb/src/test/scala/akka/stream/alpakka/mongodb/MongoSinkSpec.scala) { #updateOne }

#### Delete
We can delete documents with a Source of filters. Use either @scaladoc[deleteOne](akka.stream.alpakka.mongodb.scaladsl.MongoSink$#deleteOne) or @scaladoc[deleteMany](akka.stream.alpakka.mongodb.scaladsl.MongoSink$#deleteMany) if the filter should target one or many documents.

Scala
: @@snip (../../../../mongodb/src/test/scala/akka/stream/alpakka/mongodb/MongoSinkSpec.scala) { #deleteOne }

### Running the example code

The code in this guide is part of runnable tests of this project. You are welcome to edit the code and run it in sbt.

> Test code requires a MongoDB server running in the background. You can start one quickly using docker:
>
> `docker-compose up mongo`

Scala
:   ```
    sbt
    > mongodb/test
    ```
