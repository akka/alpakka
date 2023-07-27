# Google Cloud BigQuery Storage

The BigQuery Storage API offers fast access to BigQuery-managed storage using an [rpc-based](https://cloud.google.com/bigquery/docs/reference/storage/rpc) protocol.
It is seen as an improvement over the REST API, and bulk data `extract` jobs for accessing BigQuery-managed table data, but doesn't offer any functionality around managing BigQuery resources.
Further information at the official [Google Cloud documentation website](https://cloud.google.com/bigquery/docs/reference/storage).

This connector communicates to the BigQuery Storage API via the gRPC protocol. The integration between Akka Stream and gRPC is handled by the
@extref:[Akka gRPC library](akka-grpc:/). Currently, this connector only supports returning each row as an Avro GenericRecord.

@@project-info{ projectId="google-cloud-bigquery-storage" }

## Artifacts

The Akka dependencies are available from Akka's library repository. To access them there, you need to configure the URL for this repository.

@@repository [sbt,Maven,Gradle] {
id="akka-repository"
name="Akka library repository"
url="https://repo.akka.io/maven"
}

Additionally, add the dependencies as below. Since Akka gRPC uses Akka Discovery internally. Make sure to add Akka Discovery with the same Akka version that the application uses.

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

HTTP/2 requires ALPN negotiation, which comes with the JDK starting with
version 8u251.

For older versions of the JDK you will need to load the `jetty-alpn-agent`
yourself, but we recommend upgrading.

@@@

## Configuration

The BigQuery Storage connector @ref[shares its basic configuration](google-common.md) with all the Google connectors in Alpakka.

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

A manually initialized @scala[`akka.stream.alpakka.googlecloud.bigquery.storage.scaladsl.GrpcBigQueryStorageReader`]@java[`akka.stream.alpakka.googlecloud.bigquery.storage.javadsl.GrpcBigQueryStorageReader`] can be used by providing it as an attribute to the stream:

Scala
: @@snip (/google-cloud-bigquery-storage/src/test/scala/docs/scaladsl/ExampleReader.scala) { #attributes }

Java
: @@snip (/google-cloud-bigquery-storage/src/test/java/docs/javadsl/ExampleReader.java) { #attributes }

## Reading

We can read in a number of ways. To read data from a table a read session needs to be created. 
On the session creation we can specify the number of streams to be used in order to transfer the data, this makes it feasible to achieve parallelism while ingesting the data, thus achieving better performance.
To create a session the data format needs to be specified. The options provided are Avro and Arrow.

If no `TableReadOptions` are specified all the table's columns shall be retrieved as a `Source` containing a `Source` for each stream, which will each deliver a section of the rows:

Scala
: @@snip (/google-cloud-bigquery-storage/src/test/scala/docs/scaladsl/ExampleReader.scala) { #read-all }

Java
: @@snip (/google-cloud-bigquery-storage/src/test/java/docs/javadsl/ExampleReader.java) { #read-all }

Secondly, by specifying [`TableReadOptions`](https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#tablereadoptions), we can narrow down the amount of data returned, filtering down the columns returned, and/or a `row_restriction`. This is defined as:

> SQL text filtering statement, similar to a WHERE clause in a query. Currently, only a single predicate that is a comparison between a column and a constant value is supported. Aggregates are not supported.

Scala
: @@snip (/google-cloud-bigquery-storage/src/test/scala/docs/scaladsl/ExampleReader.scala) { #read-options }

Java
: @@snip (/google-cloud-bigquery-storage/src/test/java/docs/javadsl/ExampleReader.java) { #read-options }

You can then choose to read and process these streams as is or merged. 
You can process the streams merged in rows. You need to provide a `ByteString` `Unmarshaller` based on the format requested.

Scala
: @@snip (/google-cloud-bigquery-storage/src/test/scala/docs/scaladsl/ExampleReader.scala) { #read-merged}

Java
: @@snip (/google-cloud-bigquery-storage/src/test/java/docs/javadsl/ExampleReader.java) { #read-merged }

Or process the stream of rows individually:

Scala
: @@snip (/google-cloud-bigquery-storage/src/test/scala/docs/scaladsl/ExampleReader.scala) { #read-all }

Java
: @@snip (/google-cloud-bigquery-storage/src/test/java/docs/javadsl/ExampleReader.java) { #read-all }

Since Avro and Arrow are the formats available, streams for those specific formats can be created.

You can read Arrow Record streams merged 

Scala
: @@snip (/google-cloud-bigquery-storage/src/test/scala/docs/scaladsl/ExampleReader.scala) { #read-arrow-merged }

Java
: @@snip (/google-cloud-bigquery-storage/src/test/java/docs/javadsl/ExampleReader.java) { #read-arrow-merged }

You can read Arrow Record streams individually 

Scala
: @@snip (/google-cloud-bigquery-storage/src/test/scala/docs/scaladsl/ExampleReader.scala) { #read-arrow-all }

Java
: @@snip (/google-cloud-bigquery-storage/src/test/java/docs/javadsl/ExampleReader.java) { #read-arrow-all }

You can read Avro Record streams merged

Scala
: @@snip (/google-cloud-bigquery-storage/src/test/scala/docs/scaladsl/ExampleReader.scala) { #read-avro-merged }

Java
: @@snip (/google-cloud-bigquery-storage/src/test/java/docs/javadsl/ExampleReader.java) { #read-avro-merged }

You can read Avro Record streams individually

Scala
: @@snip (/google-cloud-bigquery-storage/src/test/scala/docs/scaladsl/ExampleReader.scala) { #read-avro-all }

Java
: @@snip (/google-cloud-bigquery-storage/src/test/java/docs/javadsl/ExampleReader.java) { #read-avro-all }



## Running the test code
The tests use a [`BigQueryMockServer`](/google-cloud-bigquery-storage/src/test/scala/akka/stream/alpakka/bigquery/storage/mock/BigQueryMockServer.scala) that implements the server defined in the protobuf for the Storage API. It essentially provides a mock table on which to query.
Tests can be started from sbt by running:

sbt
:   ```bash
    > google-cloud-bigquery-storage/test
    ```
