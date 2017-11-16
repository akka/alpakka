# MongoDB Connector

The MongoDB connector provides a way to provide the result of a MongoDB query as a stream of documents.

## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-mongodb_$scalaBinaryVersion$
  version=$version$
}

## Usage

Sources provided by this connector need a prepared session to communicate with MongoDB server. First, lets initialize a MongoDB connection.

Scala
: @@snip ($alpakka$/mongodb/src/test/scala/akka/stream/alpakka/mongodb/MongoSourceSpec.scala) { #init-connection }

We will also need an @scaladoc[ActorSystem](akka.actor.ActorSystem) and an @scaladoc[ActorMaterializer](akka.stream.ActorMaterializer).

Scala
: @@snip ($alpakka$/mongodb/src/test/scala/akka/stream/alpakka/mongodb/MongoSourceSpec.scala) { #init-mat }

This is all preparation that we are going to need.

### Source Usage

Let's create a source from a MongoDB collection observable, which can optionally take a filter.

Scala
: @@snip ($alpakka$/mongodb/src/test/scala/akka/stream/alpakka/mongodb/MongoSourceSpec.scala) { #create-source }

And finally we can run it.

Scala
: @@snip ($alpakka$/mongodb/src/test/scala/akka/stream/alpakka/mongodb/MongoSourceSpec.scala) { #run-source }

Here we used a basic sink to complete the stream by collecting all of the stream elements to a collection. The power of streams comes from building larger data pipelines which leverage backpressure to ensure efficient flow control. Feel free to edit the example code and build @extref[more advanced stream topologies](akka-docs:scala/stream/stream-introduction).

### Running the example code

The code in this guide is part of runnable tests of this project. You are welcome to edit the code and run it in sbt.

> Test code requires MongoDB server running in the background. You can start one quickly using docker:
>
> `docker run --rm mongo`

Scala
:   ```
    sbt
    > mongodb/test
    ```
