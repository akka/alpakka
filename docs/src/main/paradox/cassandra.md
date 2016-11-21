# Cassandra Connector

The Cassandra connector provides a way to provide the result of a Cassandra query as a stream of rows.

## Artifacts

sbt
:   @@@vars
    ```scala
    libraryDependencies += "com.lightbend.akka" %% "akka-stream-alpakka-cassandra" % "$version$"
    ```
    @@@

Maven
:   @@@vars
    ```xml
    <dependency>
      <groupId>com.lightbend.akka</groupId>
      <artifactId>akka-stream-alpakka-cassandra_$scala.binaryVersion$</artifactId>
      <version>$version$</version>
    </dependency>
    ```
    @@@

Gradle
:   @@@vars
    ```gradle
    dependencies {
      compile group: "com.lightbend.akka", name: "akka-stream-alpakka-cassandra_$scala.binaryVersion$", version: "$version$"
    }
    ```
    @@@

## Usage

Sources provided by this connector need a prepared session to communicate with Cassandra cluster. First, lets initialize a Cassandra session.

Scala
: @@snip (../../../../cassandra/src/test/scala/akka/stream/alpakka/cassandra/scaladsl/CassandraSourceSpec.scala) { #init-session }

Java
: @@snip (../../../../cassandra/src/test/java/akka/stream/alpakka/cassandra/javadsl/CassandraSourceTest.java) { #init-session }

We will also need an @scaladoc[ActorSystem](akka.actor.ActorSystem) and an @scaladoc[ActorMaterializer](akka.stream.ActorMaterializer).

Scala
: @@snip (../../../../cassandra/src/test/scala/akka/stream/alpakka/cassandra/scaladsl/CassandraSourceSpec.scala) { #init-mat }

Java
: @@snip (../../../../cassandra/src/test/java/akka/stream/alpakka/cassandra/javadsl/CassandraSourceTest.java) { #init-mat }

This is all preparation that we are going to need.

Let's create a Cassandra statement with a query that we want to execute.

Scala
: @@snip (../../../../cassandra/src/test/scala/akka/stream/alpakka/cassandra/scaladsl/CassandraSourceSpec.scala) { #statement }

Java
: @@snip (../../../../cassandra/src/test/java/akka/stream/alpakka/cassandra/javadsl/CassandraSourceTest.java) { #statement }

And finally create the source using any method from the @scaladoc[CassandraSource](akka.stream.alpakka.cassandra.CassandraSource$) factory and run it.

Scala
: @@snip (../../../../cassandra/src/test/scala/akka/stream/alpakka/cassandra/scaladsl/CassandraSourceSpec.scala) { #run-source }

Java
: @@snip (../../../../cassandra/src/test/java/akka/stream/alpakka/cassandra/javadsl/CassandraSourceTest.java) { #run-source }

Here we used a basic sink to complete the stream by collecting all of the stream elements to a collection. The power of streams comes from building larger data pipelines which leverage backpressure to ensure efficient flow control. Feel free to edit the example code and build @extref[more advanced stream topologies](akka-docs:scala/stream/stream-introduction).

### Running the example code

The code in this guide is part of runnable tests of this project. You are welcome to edit the code and run it in sbt.

> Test code requires Cassandra server running in the background. You can start one quickly using docker:
>
> `docker run --rm -p 9042:9042 cassandra:3`

Scala
:   ```
    sbt
    > cassandra/testOnly *.CassandraSourceSpec
    ```

Java
:   ```
    sbt
    > cassandra/testOnly *.CassandraSourceTest
    ```
