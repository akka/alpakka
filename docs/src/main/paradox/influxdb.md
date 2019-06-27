#InfluxDB

The Alpakka InfluxDb connector provides Akka Streams integration for InfluxDB.

For more information about InfluxDB, please visit the [InfluxDB Documentation](https://docs.influxdata.com/)

@@project-info{ projectId="influxdb" }

## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-influxdb$scala.binary.version$
  version=$project.version$
}

The table below shows direct dependencies of this module and the second tab shows all libraries it depends on transitively.

@@dependencies { projectId="influxdb" }

## Set up InfluxDB client

Sources, Flows and Sinks provided by this connector need a prepared `org.influxdb.InfluxDB` to
access to InfluxDB.

Scala
: @@snip [snip](/influxdb/src/test/scala/docs/scaladsl/InfluxDbSpec.scala) { #init-client }

Java
: @@snip [snip](/influxdb/src/test/java/docs/javadsl/TestUtils.java) { #init-client }

## InfluxDB as Source and Sink

Now we can stream messages from or to InfluxDB by providing the `InfluxDB` to the
@scala[@scaladoc[InfluxDbSource](akka.stream.alpakka.influxdb.scaladsl.InfluxDbSource$)]
@java[@scaladoc[InfluxDbSource](akka.stream.alpakka.influxdb.javadsl.InfluxDbSource$)]
or the
@scala[@scaladoc[InfluxDbSink](akka.stream.alpakka.influxdb.scaladsl.InfluxDbSink$).]
@java[@scaladoc[InfluxDbSink](akka.stream.alpakka.influxdb.javadsl.InfluxDbSink$).]


Scala
: @@snip [snip](/influxdb/src/test/scala/docs/scaladsl/InfluxDbSpecCpu.java) { #define-class }

Java
: @@snip [snip](/influxdb/src/test/java/docs/javadsl/InfluxDbCpu.java) { #define-class }

### With typed source

Use `InfluxDbSource.typed` and `InfluxDbSink.typed` to create source and sink.
@scala[The data is converted by InfluxDBMapper.]
@java[The data is converted by InfluxDBMapper.]

Scala
: @@snip [snip](/influxdb/src/test/scala/docs/scaladsl/InfluxDbSpec.scala) { #run-typed }

Java
: @@snip [snip](/influxdb/src/test/java/docs/javadsl/InfluxDbTest.java) { #run-typed }

### With `QueryResult` source

Use `InfluxDbSource.create` and `InfluxDbSink.create` to create source and sink.

Scala
: @@snip [snip](/influxdb/src/test/scala/docs/scaladsl/InfluxDbSpec.scala) { #run-query-result}

Java
: @@snip [snip](/influxdb/src/test/java/docs/javadsl/InfluxDbTest.java) { #run-query-result}

TODO

### Writing to InfluxDB

You can also build flow stages. 
@scala[@scaladoc[InfluxDbFlow](akka.stream.alpakka.influxdb.scaladsl.InfluxDbFlow$).]
@java[@scaladoc[InfluxDbFlow](akka.stream.alpakka.influxdb.javadsl.InfluxDbFlow$).]
The API is similar to creating Sinks.

Scala
: @@snip [snip](/influxdb/src/test/scala/docs/scaladsl/FlowSpec.scala) { #run-flow }

Java
: @@snip [snip](/influxdb/src/test/java/docs/javadsl/InfluxDbTest.java) { #run-flow }

### Passing data through InfluxDbFlow 

When streaming documents from Kafka, you might want to commit to Kafka **AFTER** the document has been written to InfluxDB.

Scala
: @@snip [snip](/influxdb/src/test/scala/docs/scaladsl/FlowSpec.scala) { #kafka-example }

Java
: @@snip [snip](/influxdb/src/test/java/docs/javadsl/InfluxDbTest.java) { #kafka-example }


