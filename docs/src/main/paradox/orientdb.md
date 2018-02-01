# OrientDB Connector

The OrientDB connector provides Akka Stream sources and sinks for OrientDB.

For more information about OrientDB please visit the [official documentation](http://orientdb.com/orientdb/).

## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-orientdb_$scalaBinaryVersion$
  version=$version$
}

## Usage

Sources, Flows and Sinks provided by this connector need dbUrl & credentials to access to OrientDB.

Scala
: @@snip ($alpakka$/orientdb/src/test/scala/akka/stream/alpakka/orientdb/OrientDBSpec.scala) { #init-settings }

Java
: @@snip ($alpakka$/orientdb/src/test/java/akka/stream/alpakka/orientdb/OrientDBTest.java) { #init-settings }

We will also need an @scaladoc[ActorSystem](akka.actor.ActorSystem) and an @scaladoc[ActorMaterializer](akka.stream.ActorMaterializer).

Scala
: @@snip ($alpakka$/orientdb/src/test/scala/akka/stream/alpakka/orientdb/OrientDBSpec.scala) { #init-mat }

Java
: @@snip ($alpakka$/orientdb/src/test/java/akka/stream/alpakka/orientdb/OrientDBTest.java) { #init-mat }

This is all preparation that we are going to need.

## ODocument message

Now we can stream messages which contain OrientDB's `ODocument` (in Scala or Java)
from or to OrientDB by providing the `ODatabaseDocumentTx` to the
@scaladoc[OrientDBSource](akka.stream.alpakka.orientdb.scaladsl.OrientDBSource$) or the
@scaladoc[OrientDBSink](akka.stream.alpakka.orientdb.scaladsl.OrientDBSink$).

Scala
: @@snip ($alpakka$/orientdb/src/test/scala/akka/stream/alpakka/orientdb/OrientDBSpec.scala) { #run-odocument }

Java
: @@snip ($alpakka$/orientdb/src/test/java/akka/stream/alpakka/orientdb/OrientDBTest.java) { #run-odocument }


## Typed messages

Also, it's possible to stream messages which contains any classes. 

Java
: @@snip ($alpakka$/orientdb/src/test/java/akka/stream/alpakka/orientdb/OrientDBTest.java) { #define-class }


Use `OrientDBSource.typed` and `OrientDBSink.typed` to create source and sink instead.

Java
: @@snip ($alpakka$/orientdb/src/test/java/akka/stream/alpakka/orientdb/OrientDBTest.java) { #run-typed }


## Configuration

We can configure the source by `OrientDBSourceSettings`.

Scala (source)
: @@snip ($alpakka$/orientdb/src/main/scala/akka/stream/alpakka/orientdb/OrientDBSourceSettings.scala) { #source-settings }

| Parameter        | Default | Description                                                                                                              |
| ---------------- | ------- | ------------------------------------------------------------------------------------------------------------------------ |
| maxPartitionSize |         | `OrientDBSource` and `OrientDBSink` uses this for initializing DB Connections. |
| maxPoolSize      |    -1   | `OrientDBSource` and `OrientDBSink` uses this for initializing DB Connections. |
| skip             |         | `OrientDBSource` uses this property to fetch data from the DB. |
| limit            |         | `OrientDBSource` uses this property to fetch data from the DB. |
| dbUrl            |         | url to the OrientDB database. |
| username         |         | username to connect to OrientDB. |
| password         |         | password to connect to OrientDB. | 

Also, we can configure the sink by `OrientDBUpdateSettings`.

Scala (sink)
: @@snip ($alpakka$/orientdb/src/main/scala/akka/stream/alpakka/orientdb/OrientDBUpdateSettings.scala) { #sink-settings }

| Parameter           | Default | Description                                                                                            |
| ------------------- | ------- | ------------------------------------------------------------------------------------------------------ |
| maxPartitionSize |         | `OrientDBSource` and `OrientDBSink` uses this for initializing DB Connections. |
| maxPoolSize      |    -1   | `OrientDBSource` and `OrientDBSink` uses this for initializing DB Connections. |
| maxRetry         |     1   | `OrientDBSink` uses this for retrying write operations to OrientDB. |
| retryInterval    |  5000   | `OrientDBSink` uses this for retrying write operations to OrientDB. |
| bufferSize       |         | `OrientDBSink` uses this for retrieving data from DB. |
| dbUrl            |         | url to the OrientDB database. |
| username         |         | username to connect to OrientDB. |
| password         |         | password to connect to OrientDB. | 

## Using OrientDB as a Flow

You can also build flow stages. The API is similar to creating Sinks.

Scala (flow)
: @@snip ($alpakka$/orientdb/src/test/scala/akka/stream/alpakka/orientdb/OrientDBSpec.scala) { #run-flow }

Java (flow)
: @@snip ($alpakka$/orientdb/src/test/java/akka/stream/alpakka/orientdb/OrientDBTest.java) { #run-flow }

### Passing data through OrientDBFlow

When streaming documents from Kafka, you might want to commit to Kafka **AFTER** the document has been written to OrientDB.

Scala
: @@snip ($alpakka$/orientdb/src/test/scala/akka/stream/alpakka/orientdb/OrientDBSpec.scala) { #kafka-example }

Java
: @@snip ($alpakka$/orientdb/src/test/java/akka/stream/alpakka/orientdb/OrientDBTest.java) { #kafka-example } 

## Running the example code

The code in this guide is part of runnable tests of this project. You are welcome to edit the code and run it in sbt.

  > Test code requires OrientDB server running in the background. You can start one quickly using docker:
  >		  
  > `docker run --rm -p 2424:2424 orientdb:latest`

Scala
:   ```
    sbt
    > orientdb/testOnly *.OrientDBSpec
    ```

Java
:   ```
    sbt
    > orientdb/testOnly *.OrientDBTest
    ```
