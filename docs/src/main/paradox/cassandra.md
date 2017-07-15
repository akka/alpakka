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
      <artifactId>akka-stream-alpakka-cassandra_$scalaBinaryVersion$</artifactId>
      <version>$version$</version>
    </dependency>
    ```
    @@@

Gradle
:   @@@vars
    ```gradle
    dependencies {
      compile group: "com.lightbend.akka", name: "akka-stream-alpakka-cassandra_$scalaBinaryVersion$", version: "$version$"
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

### Source Usage

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

### Sink Usage

Let's create a Cassandra Prepared statement with a query that we want to execute.

Scala
: @@snip (../../../../cassandra/src/test/scala/akka/stream/alpakka/cassandra/scaladsl/CassandraSourceSpec.scala) { #prepared-statement }

Java
: @@snip (../../../../cassandra/src/test/java/akka/stream/alpakka/cassandra/javadsl/CassandraSourceTest.java) { #prepared-statement }

Now lets we need to create a 'statement binder', this is just a function to bind to the prepared statement. It can take in any type / data structure to fit your query values. Here we're just using one Integer, but it can just as easily be a (case) class.

Scala
: @@snip (../../../../cassandra/src/test/scala/akka/stream/alpakka/cassandra/scaladsl/CassandraSourceSpec.scala) { #statement-binder }

Java
: @@snip (../../../../cassandra/src/test/java/akka/stream/alpakka/cassandra/javadsl/CassandraSourceTest.java) { #statement-binder }

Finally we run the sink from any source.

Scala
: @@snip (../../../../cassandra/src/test/scala/akka/stream/alpakka/cassandra/scaladsl/CassandraSourceSpec.scala) { #run-sink }

Java
: @@snip (../../../../cassandra/src/test/java/akka/stream/alpakka/cassandra/javadsl/CassandraSourceTest.java) { #run-sink }

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
