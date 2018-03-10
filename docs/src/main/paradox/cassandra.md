# Cassandra Connector

The Cassandra connector allows you to read and write to Cassandra. You can query a stream of rows from @scaladoc[CassandraSource](akka.stream.alpakka.cassandra.scaladsl.CassandraSource$) or use prepared statements to insert or update with @scaladoc[CassandraFlow](akka.stream.alpakka.cassandra.scaladsl.CassandraFlow$) or @scaladoc[CassandraSink](akka.stream.alpakka.cassandra.scaladsl.CassandraSink$).

Unlogged batches are also supported. 


### Reported issues

[Tagged issues at Github](https://github.com/akka/alpakka/labels/p%3Acassandra)


## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-cassandra_$scalaBinaryVersion$
  version=$version$
}

## Usage

Sources provided by this connector need a prepared session to communicate with Cassandra cluster. First, let's initialize a Cassandra session.

Scala
: @@snip ($alpakka$/cassandra/src/test/scala/akka/stream/alpakka/cassandra/scaladsl/CassandraSourceSpec.scala) { #init-session }

Java
: @@snip ($alpakka$/cassandra/src/test/java/akka/stream/alpakka/cassandra/javadsl/CassandraSourceTest.java) { #init-session }

We will also need an @scaladoc[ActorSystem](akka.actor.ActorSystem) and an @scaladoc[ActorMaterializer](akka.stream.ActorMaterializer).

Scala
: @@snip ($alpakka$/cassandra/src/test/scala/akka/stream/alpakka/cassandra/scaladsl/CassandraSourceSpec.scala) { #init-mat }

Java
: @@snip ($alpakka$/cassandra/src/test/java/akka/stream/alpakka/cassandra/javadsl/CassandraSourceTest.java) { #init-mat }

This is all preparation that we are going to need.

### Source Usage

Let's create a Cassandra statement with a query that we want to execute.

Scala
: @@snip ($alpakka$/cassandra/src/test/scala/akka/stream/alpakka/cassandra/scaladsl/CassandraSourceSpec.scala) { #statement }

Java
: @@snip ($alpakka$/cassandra/src/test/java/akka/stream/alpakka/cassandra/javadsl/CassandraSourceTest.java) { #statement }

And finally create the source using any method from the @scaladoc[CassandraSource](akka.stream.alpakka.cassandra.CassandraSource$) factory and run it.

Scala
: @@snip ($alpakka$/cassandra/src/test/scala/akka/stream/alpakka/cassandra/scaladsl/CassandraSourceSpec.scala) { #run-source }

Java
: @@snip ($alpakka$/cassandra/src/test/java/akka/stream/alpakka/cassandra/javadsl/CassandraSourceTest.java) { #run-source }

Here we used a basic sink to complete the stream by collecting all of the stream elements to a collection. The power of streams comes from building larger data pipelines which leverage backpressure to ensure efficient flow control. Feel free to edit the example code and build @extref[more advanced stream topologies](akka-docs:scala/stream/stream-introduction).

### Flow with passthrough Usage

Let's create a Cassandra Prepared statement with a query that we want to execute.

Scala
: @@snip ($alpakka$/cassandra/src/test/scala/akka/stream/alpakka/cassandra/scaladsl/CassandraSourceSpec.scala) { #prepared-statement-flow }

Java
: @@snip ($alpakka$/cassandra/src/test/java/akka/stream/alpakka/cassandra/javadsl/CassandraSourceTest.java) { #prepared-statement-flow }

Now we need to create a 'statement binder', this is just a function to bind to the prepared statement. It can take in any type / data structure to fit your query values. Here we're just using one Integer, but it can just as easily be a (case) class.

Scala
: @@snip ($alpakka$/cassandra/src/test/scala/akka/stream/alpakka/cassandra/scaladsl/CassandraSourceSpec.scala) { #statement-binder-flow }

Java
: @@snip ($alpakka$/cassandra/src/test/java/akka/stream/alpakka/cassandra/javadsl/CassandraSourceTest.java) { #statement-binder-flow }

We run the stream persisting the elements to C* and finally folding them using a ```Sink.fold```.

Scala
: @@snip ($alpakka$/cassandra/src/test/scala/akka/stream/alpakka/cassandra/scaladsl/CassandraSourceSpec.scala) { #run-flow }

Java
: @@snip ($alpakka$/cassandra/src/test/java/akka/stream/alpakka/cassandra/javadsl/CassandraSourceTest.java) { #run-flow }

### Flow with passthrough and unlogged batching Usage

Use this when most of the elements in the stream share the same partition key. 

Cassandra unlogged batches that share the same partition key will only
resolve to one write internally in Cassandra, boosting write performance.

**Be aware that this stage does not preserve the upstream order!**

For this example we will define a class that model the data to be inserted

Scala
: @@snip ($alpakka$/cassandra/src/test/scala/akka/stream/alpakka/cassandra/scaladsl/CassandraSourceSpec.scala) { #element-to-insert }

Java
: @@snip ($alpakka$/cassandra/src/test/java/akka/stream/alpakka/cassandra/javadsl/CassandraSourceTest.java) { #element-to-insert }


Let's create a Cassandra Prepared statement with a query that we want to execute.

Scala
: @@snip ($alpakka$/cassandra/src/test/scala/akka/stream/alpakka/cassandra/scaladsl/CassandraSourceSpec.scala) { #prepared-statement-batching-flow }

Java
: @@snip ($alpakka$/cassandra/src/test/java/akka/stream/alpakka/cassandra/javadsl/CassandraSourceTest.java) { #prepared-statement-batching-flow }

Now we need to create a 'statement binder', this is just a function to bind to the prepared statement. In this example we are using a class.

Scala
: @@snip ($alpakka$/cassandra/src/test/scala/akka/stream/alpakka/cassandra/scaladsl/CassandraSourceSpec.scala) { #statement-binder-batching-flow }

Java
: @@snip ($alpakka$/cassandra/src/test/java/akka/stream/alpakka/cassandra/javadsl/CassandraSourceTest.java) { #statement-binder-batching-flow }

You can define the amount of grouped elements, in this case we will use the default ones:

Scala
: @@snip ($alpakka$/cassandra/src/test/scala/akka/stream/alpakka/cassandra/scaladsl/CassandraSourceSpec.scala) { #settings-batching-flow }

Java
: @@snip ($alpakka$/cassandra/src/test/java/akka/stream/alpakka/cassandra/javadsl/CassandraSourceTest.java) { #settings-batching-flow }


We run the stream persisting the elements to C* and finally folding them using a ```Sink.fold```. The function T => K has to extract the Cassandra partition key from your class.

Scala
: @@snip ($alpakka$/cassandra/src/test/scala/akka/stream/alpakka/cassandra/scaladsl/CassandraSourceSpec.scala) { #run-batching-flow }

Java
: @@snip ($alpakka$/cassandra/src/test/java/akka/stream/alpakka/cassandra/javadsl/CassandraSourceTest.java) { #run-batching-flow }


### Sink Usage

Let's create a Cassandra Prepared statement with a query that we want to execute.

Scala
: @@snip ($alpakka$/cassandra/src/test/scala/akka/stream/alpakka/cassandra/scaladsl/CassandraSourceSpec.scala) { #prepared-statement }

Java
: @@snip ($alpakka$/cassandra/src/test/java/akka/stream/alpakka/cassandra/javadsl/CassandraSourceTest.java) { #prepared-statement }

Now we need to create a 'statement binder', this is just a function to bind to the prepared statement. It can take in any type / data structure to fit your query values. Here we're just using one Integer, but it can just as easily be a (case) class.

Scala
: @@snip ($alpakka$/cassandra/src/test/scala/akka/stream/alpakka/cassandra/scaladsl/CassandraSourceSpec.scala) { #statement-binder }

Java
: @@snip ($alpakka$/cassandra/src/test/java/akka/stream/alpakka/cassandra/javadsl/CassandraSourceTest.java) { #statement-binder }

Finally we run the sink from any source.

Scala
: @@snip ($alpakka$/cassandra/src/test/scala/akka/stream/alpakka/cassandra/scaladsl/CassandraSourceSpec.scala) { #run-sink }

Java
: @@snip ($alpakka$/cassandra/src/test/java/akka/stream/alpakka/cassandra/javadsl/CassandraSourceTest.java) { #run-sink }


### Running the example code

The code in this guide is part of runnable tests of this project. You are welcome to edit the code and run it in sbt.

> Test code requires Cassandra running in the background. You can start it quickly using docker:
>
> `docker-compose up cassandra`

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
