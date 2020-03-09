# Google Cloud BigQuery Storage

@@@ note
The BigQuery Storage API offers fast access to BigQuery-managed storage using an [rpc-based](https://cloud.google.com/bigquery/docs/reference/storage/rpc) protocol.
It is seen as an improvement over the REST API, and bulk data `extract` jobs for accessing BigQuery-managed table data, but doesn't offer any functionality around managing BigQuery resources.
Further information at the official [Google Cloud documentation website](https://cloud.google.com/bigquery/docs/reference/storage).
@@@

This connector communicates to the BigQuery Storage API via the gRPC protocol. The integration between Akka Stream and gRPC is handled by the
[Akka gRPC library](https://github.com/akka/akka-grpc). Currently this connector only supports returning each row as an Avro GenericRecord.
@@project-info{ projectId="google-cloud-bigquery-storage" }

## Artifacts

Akka gRPC uses Akka Discovery internally. Make sure to add Akka Discovery with the same Akka version that the application uses.

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-google-cloud-bigquery-storage_$scala.binary.version$
  version=$project.version$
  symbol2=AkkaVersion
  value2=$akka.version$
  group2=com.typesafe.akka
  artifact2=akka-stream_$scala.binary.version$
  version2=AkkaVersion
  group3=com.typesafe.akka
  artifact3=akka-discovery_$scala.binary.version$
  version3=AkkaVersion
}

The table below shows direct dependencies of this module and the second tab shows all libraries it depends on transitively.

@@dependencies { projectId="google-cloud-bigquery-storage" }

## Build setup

The Alpakka Google Cloud BigQuery Storage library contains the classes generated from [Google's protobuf specification](https://github.com/googleapis/java-bigquerystorage/tree/master/proto-google-cloud-bigquerystorage-v1).

@@@note { title="ALPN on JDK 8" }

For use on JDK 8 the ALPN Java agent needs to be set up explicitly.

@@@

### Maven

When using JDK 8: configure your project to use the Java agent for ALPN and add `-javaagent:...` to your startup scripts as described in the @extref:[Akka gRPC documentation](akka-grpc:/buildtools/maven.html#starting-your-akka-grpc-server-from-maven).

### sbt

When using JDK 8: Configure your project to use the Java agent for ALPN and add `-javaagent:...` to your startup scripts.

Pull in the [`sbt-javaagent`](https://github.com/sbt/sbt-javaagent) plugin.

project/plugins.sbt
: @@snip (/project/plugins.sbt) { #grpc-agent }

Enable the Akka gRPC and JavaAgent plugins on the sbt project.

build.sbt
: @@snip (/build.sbt) { #grpc-plugins }

Add the Java agent to the runtime configuration.

build.sbt
:   ```scala
    javaAgents += "org.mortbay.jetty.alpn" % "jetty-alpn-agent" % "2.0.9"
    ```

### Gradle

When using JDK 8: Configure your project to use the Java agent for ALPN and add `-javaagent:...` to your startup scripts as described in the @extref:[Akka gRPC documentation](akka-grpc:/buildtools/gradle.html#starting-your-akka-grpc-server-from-gradle).

## Configuration

The connector comes with the default settings configured to work with the Google BigQuery endpoint and uses the default way of
locating credentials by looking at the `GOOGLE_APPLICATION_CREDENTIALS` environment variable. Please check
[Google official documentation](https://cloud.google.com/bigquery/docs/reference/libraries#setting_up_authentication) for more details
on how to obtain credentials for your application.

The defaults can be changed (for example when testing against a local implementation of the server) by tweaking the reference configuration:

reference.conf
: @@snip (/google-cloud-bigquery-storage/src/main/resources/reference.conf)

Example Test Configuration
```
alpakka.google.cloud.bigquery.grpc {
  host = "localhost"
  port = 21000
  rootCa = "none"
  callCredentials = "none"
}
```

For more configuration details consider the underlying configuration for @extref:[Akka gRPC](akka-grpc:/client/configuration.html).

A manually initialized @scala[@scaladoc[GrpcBigQueryStorageReader](akka.stream.alpakka.googlecloud.bigquery.storage.scaladsl.GrpcBigQueryStorageReader)]@java[@scaladoc[GrpcBigQueryStorageReader](akka.stream.alpakka.googlecloud.bigquery.storage.javadsl.GrpcBigQueryStorageReader)] can be used by providing it as an attribute to the stream:

Scala
: @@snip (/google-cloud-bigquery-storage/src/test/scala/docs/scaladsl/ExampleReader.scala) { #attributes }

Java
: @@snip (/google-cloud-bigquery-storage/src/test/java/docs/javadsl/ExampleReader.java) { #attributes }

## Reading

We can read in a number of ways, depending on the parallelism required on consumption. This parallelism is one big advantage over the REST APIs
First of all, specifying no TableReadOptions will return the whole table, as a Source containing a Source for each stream, which will each deliver a section of the rows:
Scala
: @@snip (/google-cloud-bigquery-storage/src/test/scala/docs/scaladsl/ExampleReader.scala) { #read-all }

Java
: @@snip (/google-cloud-bigquery-storage/src/test/java/docs/javadsl/ExampleReader.java) { #read-all }

Secondly, by specifying [TableReadOptions](https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#tablereadoptions), we can narrow down the amount of data returned, filtering down the columns returned, and/or a `row_restriction`. This is defined as:
> SQL text filtering statement, similar to a WHERE clause in a query. Currently, only a single predicate that is a comparison between a column and a constant value is supported. Aggregates are not supported
Scala
: @@snip (/google-cloud-bigquery-storage/src/test/scala/docs/scaladsl/ExampleReader.scala) { #read-options }

Java
: @@snip (/google-cloud-bigquery-storage/src/test/java/docs/javadsl/ExampleReader.java) { #read-options }

You can then choose to read and process these streams sequentially, essentially with no parallelism:
Scala
: @@snip (/google-cloud-bigquery-storage/src/test/scala/docs/scaladsl/ExampleReader.scala) { #read-sequential }

Java
: @@snip (/google-cloud-bigquery-storage/src/test/java/docs/javadsl/ExampleReader.java) { #read-sequential }

Or in parallel:
Scala
: @@snip (/google-cloud-bigquery-storage/src/test/scala/docs/scaladsl/ExampleReader.scala) { #read-parallel }

Java
: @@snip (/google-cloud-bigquery-storage/src/test/java/docs/javadsl/ExampleReader.java) { #read-parallel }


## Running the test code
The tests use a [`BigQueryMockServer`](/google-cloud-bigquery-storage/src/test/scala/akka/stream/alpakka/bigquery/storage/mock/BigQueryMockServer.scala) that implements the server defined in the protobuf for the Storage API. It essentially provides a mock table on which to query.
Tests can be started from sbt by running:

sbt
:   ```bash
    > google-cloud-bigquery-storage/test
    ```
