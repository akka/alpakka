# OrientDB

@@@ note { title="OrientDB" }

OrientDB is a multi-model database, supporting graph, document, key/value, and object models, but the relationships are managed as in graph databases with direct connections between records. It supports schema-less, schema-full and schema-mixed modes. It has a strong security profiling system based on users and roles and supports querying with Gremlin along with SQL extended for graph traversal.

For more information about OrientDB please visit the [official documentation](http://orientdb.com/orientdb/), more details are available in [the OrientDB manual](http://orientdb.com/docs/3.0.x/).

@@@

The Alpakka OrientDB connector provides Akka Stream sources and sinks for OrientDB.


@@project-info{ projectId="orientdb" }


## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-orientdb_$scala.binary.version$
  version=$project.version$
  symbol2=AkkaVersion
  value2=$akka.version$
  group2=com.typesafe.akka
  artifact2=akka-stream_$scala.binary.version$
  version2=AkkaVersion
}

The table below shows direct dependencies of this module and the second tab shows all libraries it depends on transitively.

@@dependencies { projectId="orientdb" }

## Database connection

Sources, Flows and Sinks provided by this connector need a `OPartitionedDatabasePool` to access to OrientDB. It is your responsibility to close the database connection eg. at actor system termination. **This API has become deprecated in OrientDB, please suggest a Pull Request to use the latest APIs instead.**

Scala
: @@snip [snip](/orientdb/src/test/scala/docs/scaladsl/OrientDbSpec.scala) { #init-settings }

Java
: @@snip [snip](/orientdb/src/test/java/docs/javadsl/OrientDbTest.java) { #init-settings }


## Reading `ODocument` from OrientDB

Now we can stream messages which contain OrientDB's `ODocument` (in Scala or Java) from or to OrientDB by providing the `ODatabaseDocumentTx` to the
@scala[@scaladoc[OrientDbSource](akka.stream.alpakka.orientdb.scaladsl.OrientDbSource$)]@java[@scaladoc[OrientDbSource](akka.stream.alpakka.orientdb.javadsl.OrientDbSource$)].

Scala
: @@snip [snip](/orientdb/src/test/scala/docs/scaladsl/OrientDbSpec.scala) { #run-odocument }

Java
: @@snip [snip](/orientdb/src/test/java/docs/javadsl/OrientDbTest.java) { #run-odocument }


## Typed messages

Also, it's possible to stream messages which contains any classes. 

Java
: @@snip [snip](/orientdb/src/test/java/docs/javadsl/OrientDbTest.java) { #define-class }


Use `OrientDbSource.typed` and `OrientDbSink.typed` to create source and sink instead.

Scala
: @@snip [snip](/orientdb/src/test/scala/docs/scaladsl/OrientDbSpec.scala) { #run-typed }

Java
: @@snip [snip](/orientdb/src/test/java/docs/javadsl/OrientDbTest.java) { #run-typed }


## Source configuration

We can configure the source by `OrientDbSourceSettings`.

Scala
: @@snip [snip](/orientdb/src/test/scala/docs/scaladsl/OrientDbSpec.scala) { #source-settings }

Java
: @@snip [snip](/orientdb/src/test/java/docs/javadsl/OrientDbTest.java) { #source-settings }


| Parameter        | Default | Description |
| ---------------- | ------- | ------------------------------------------- |
| skip             |   0     | Rows skipped in the beginning of the result. |
| limit            |    10   | Result items fetched per query. |



## Writing to OrientDB

You can also build flow stages. The API is similar to creating Sinks.

Scala
: @@snip [snip](/orientdb/src/test/scala/docs/scaladsl/OrientDbSpec.scala) { #run-flow }

Java
: @@snip [snip](/orientdb/src/test/java/docs/javadsl/OrientDbTest.java) { #run-flow }


### Passing data through OrientDBFlow

When streaming documents from Kafka, you might want to commit to Kafka **AFTER** the document has been written to OrientDB.

Scala
: @@snip [snip](/orientdb/src/test/scala/docs/scaladsl/OrientDbSpec.scala) { #kafka-example }

Java
: @@snip [snip](/orientdb/src/test/java/docs/javadsl/OrientDbTest.java) { #kafka-example } 
