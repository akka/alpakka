# Apache Cassandra

The Cassandra connector allows you to read and write to Cassandra. You can query a stream of rows from @scaladoc[CassandraSource](akka.stream.alpakka.cassandra.scaladsl.CassandraSource$) or use prepared statements to insert or update with @scaladoc[CassandraFlow](akka.stream.alpakka.cassandra.scaladsl.CassandraFlow$) or @scaladoc[CassandraSink](akka.stream.alpakka.cassandra.scaladsl.CassandraSink$).

Unlogged batches are also supported. 

@@project-info{ projectId="cassandra" }

## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-cassandra_$scala.binary.version$
  version=$project.version$
}

The table below shows direct dependencies of this module and the second tab shows all libraries it depends on transitively.

@@dependencies { projectId="cassandra" }


@@@warning { title="API may change" }

We intend to bring in the Cassandra client part of [Akka Persistence Cassandra](https://github.com/akka/akka-persistence-cassandra/) to Alpakka. This will mean changes to this API.

See @github[issue #1213](#1213)

@@@

## Source

Sources provided by this connector need a prepared session to communicate with Cassandra cluster. First, let's initialize a Cassandra session.

Scala
: @@snip [snip](/cassandra/src/test/scala/docs/scaladsl/CassandraSourceSpec.scala) { #init-session }

Java
: @@snip [snip](/cassandra/src/test/java/docs/javadsl/CassandraSourceTest.java) { #init-session }

We will also need an @scaladoc[ActorSystem](akka.actor.ActorSystem) and an @scaladoc[ActorMaterializer](akka.stream.ActorMaterializer).

Scala
: @@snip [snip](/cassandra/src/test/scala/docs/scaladsl/CassandraSourceSpec.scala) { #init-mat }

Java
: @@snip [snip](/cassandra/src/test/java/docs/javadsl/CassandraSourceTest.java) { #init-mat }

Let's create a Cassandra statement with a query that we want to execute.

Scala
: @@snip [snip](/cassandra/src/test/scala/docs/scaladsl/CassandraSourceSpec.scala) { #statement }

Java
: @@snip [snip](/cassandra/src/test/java/docs/javadsl/CassandraSourceTest.java) { #statement }

And finally create the source using any method from the @scaladoc[CassandraSource](akka.stream.alpakka.cassandra.CassandraSource$) factory and run it.

Scala
: @@snip [snip](/cassandra/src/test/scala/docs/scaladsl/CassandraSourceSpec.scala) { #run-source }

Java
: @@snip [snip](/cassandra/src/test/java/docs/javadsl/CassandraSourceTest.java) { #run-source }

Here we used a basic sink to complete the stream by collecting all of the stream elements to a collection. The power of streams comes from building larger data pipelines which leverage backpressure to ensure efficient flow control. Feel free to edit the example code and build @extref[more advanced stream topologies](akka-docs:scala/stream/stream-introduction).

## Flow with passthrough

Let's create a Cassandra Prepared statement with a query that we want to execute.

Scala
: @@snip [snip](/cassandra/src/test/scala/docs/scaladsl/CassandraSourceSpec.scala) { #prepared-statement-flow }

Java
: @@snip [snip](/cassandra/src/test/java/docs/javadsl/CassandraSourceTest.java) { #prepared-statement-flow }

Now we need to create a 'statement binder', this is just a function to bind to the prepared statement. It can take in any type / data structure to fit your query values. Here we're just using one Integer, but it can just as easily be a (case) class.

Scala
: @@snip [snip](/cassandra/src/test/scala/docs/scaladsl/CassandraSourceSpec.scala) { #statement-binder-flow }

Java
: @@snip [snip](/cassandra/src/test/java/docs/javadsl/CassandraSourceTest.java) { #statement-binder-flow }

We run the stream persisting the elements to C* and finally folding them using a ```Sink.fold```.

Scala
: @@snip [snip](/cassandra/src/test/scala/docs/scaladsl/CassandraSourceSpec.scala) { #run-flow }

Java
: @@snip [snip](/cassandra/src/test/java/docs/javadsl/CassandraSourceTest.java) { #run-flow }

## Flow with passthrough and unlogged batching

Use this when most of the elements in the stream share the same partition key. 

Cassandra unlogged batches that share the same partition key will only
resolve to one write internally in Cassandra, boosting write performance.

**Be aware that this stage does not preserve the upstream order!**

For this example we will define a class that model the data to be inserted

Scala
: @@snip [snip](/cassandra/src/test/scala/docs/scaladsl/CassandraSourceSpec.scala) { #element-to-insert }

Java
: @@snip [snip](/cassandra/src/test/java/docs/javadsl/CassandraSourceTest.java) { #element-to-insert }


Let's create a Cassandra Prepared statement with a query that we want to execute.

Scala
: @@snip [snip](/cassandra/src/test/scala/docs/scaladsl/CassandraSourceSpec.scala) { #prepared-statement-batching-flow }

Java
: @@snip [snip](/cassandra/src/test/java/docs/javadsl/CassandraSourceTest.java) { #prepared-statement-batching-flow }

Now we need to create a 'statement binder', this is just a function to bind to the prepared statement. In this example we are using a class.

Scala
: @@snip [snip](/cassandra/src/test/scala/docs/scaladsl/CassandraSourceSpec.scala) { #statement-binder-batching-flow }

Java
: @@snip [snip](/cassandra/src/test/java/docs/javadsl/CassandraSourceTest.java) { #statement-binder-batching-flow }

You can define the amount of grouped elements, in this case we will use the default ones:

Scala
: @@snip [snip](/cassandra/src/test/scala/docs/scaladsl/CassandraSourceSpec.scala) { #settings-batching-flow }

Java
: @@snip [snip](/cassandra/src/test/java/docs/javadsl/CassandraSourceTest.java) { #settings-batching-flow }


We run the stream persisting the elements to C* and finally folding them using a ```Sink.fold```. The function T => K has to extract the Cassandra partition key from your class.

Scala
: @@snip [snip](/cassandra/src/test/scala/docs/scaladsl/CassandraSourceSpec.scala) { #run-batching-flow }

Java
: @@snip [snip](/cassandra/src/test/java/docs/javadsl/CassandraSourceTest.java) { #run-batching-flow }


## Sink

Let's create a Cassandra Prepared statement with a query that we want to execute.

Scala
: @@snip [snip](/cassandra/src/test/scala/docs/scaladsl/CassandraSourceSpec.scala) { #prepared-statement }

Java
: @@snip [snip](/cassandra/src/test/java/docs/javadsl/CassandraSourceTest.java) { #prepared-statement }

Now we need to create a 'statement binder', this is just a function to bind to the prepared statement. It can take in any type / data structure to fit your query values. Here we're just using one Integer, but it can just as easily be a (case) class.

Scala
: @@snip [snip](/cassandra/src/test/scala/docs/scaladsl/CassandraSourceSpec.scala) { #statement-binder }

Java
: @@snip [snip](/cassandra/src/test/java/docs/javadsl/CassandraSourceTest.java) { #statement-binder }

Finally we run the sink from any source.

Scala
: @@snip [snip](/cassandra/src/test/scala/docs/scaladsl/CassandraSourceSpec.scala) { #run-sink }

Java
: @@snip [snip](/cassandra/src/test/java/docs/javadsl/CassandraSourceTest.java) { #run-sink }


## Running the example code

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
