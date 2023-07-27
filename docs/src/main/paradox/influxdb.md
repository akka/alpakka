#InfluxDB

The Alpakka InfluxDb connector provides Akka Streams integration for InfluxDB.

For more information about InfluxDB, please visit the [InfluxDB Documentation](https://docs.influxdata.com/)

@@project-info{ projectId="influxdb" }

@@@note { title="Official Akka Streams client" }

## Influxdata, the makers of InfluxDB now offer an Akka Streams-aware client library in https://github.com/influxdata/influxdb-client-java/tree/master/client-scala 

"The reference Scala client that allows query and write for the InfluxDB 2.0 by Akka Streams."

@@@


@@@warning { title="API may change" }

Alpakka InfluxDB was added in Alpakka 1.1.0 in July 2019 and is marked as "API may change". Please try it out and suggest improvements.

Furthermore, the major InfluxDB update to [version 2.0](https://www.influxdata.com/products/influxdb) is expected to bring API and dependency changes to Alpakka InfluxDB.

@@@




## Artifacts

The Akka dependencies are available from Akka's library repository. To access them there, you need to configure the URL for this repository.

@@repository [sbt,Maven,Gradle] {
id="akka-repository"
name="Akka library repository"
url="https://repo.akka.io/maven"
}

Additionally, add the dependencies as below.

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-influxdb_$scala.binary.version$
  version=$project.version$
  symbol2=AkkaVersion
  value2=$akka.version$
  group2=com.typesafe.akka
  artifact2=akka-stream_$scala.binary.version$
  version2=AkkaVersion
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


