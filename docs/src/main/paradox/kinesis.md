# AWS Kinesis

The AWS Kinesis connector provides flows for streaming data to and from Kinesis Data streams and to Kinesis Firehose streams.

For more information about Kinesis please visit the [Kinesis documentation](https://aws.amazon.com/documentation/kinesis/).

@@@ note { title="Alternative connector 1" }

Another Kinesis connector which is based on the Kinesis Client Library is available.

The KCL Source can read from several shards and rebalance automatically when other Workers are started or stopped. It also handles record sequence checkpoints.

Please read more about it at [GitHub aserrallerios/kcl-akka-stream](https://github.com/aserrallerios/kcl-akka-stream).
@@@


@@@ note { title="Alternative connector 2" }

Another Kinesis connector which is based on the Kinesis Client Library is available.

This library combines the convenience of Akka Streams with KCL checkpoint management, failover, load-balancing, and re-sharding capabilities.

Please read more about it at [GitHub StreetContxt/kcl-akka-stream](https://github.com/StreetContxt/kcl-akka-stream).
@@@

@@@ note { title="Alternative connector 3" }

Another Kinesis connector which is based on the Kinesis Client Library 2.x is available.

This library exposes an Akka Streams Source backed by the KCL for checkpoint management, failover, load-balancing, and re-sharding capabilities.

Please read more about it at [GitHub 500px/kinesis-stream](https://github.com/500px/kinesis-stream).
@@@

@@project-info{ projectId="kinesis" }

## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-kinesis_$scala.binary.version$
  version=$project.version$
}

The table below shows direct dependencies of this module and the second tab shows all libraries it depends on transitively.

@@dependencies { projectId="kinesis" }


## Kinesis Data Streams

### Create the Kinesis client

Sources and Flows provided by this connector need a `KinesisAsyncClient` instance to consume messages from a shard.

@@@ note
The `KinesisAsyncClient` instance you supply is thread-safe and can be shared amongst multiple `GraphStages`. 
As a result, individual `GraphStages` will not automatically shutdown the supplied client when they complete.
It is recommended to shut the client instance down on Actor system termination.
@@@

Scala
: @@snip [snip](/kinesis/src/test/scala/docs/scaladsl/KinesisSnippets.scala) { #init-client }

Java
: @@snip [snip](/kinesis/src/test/java/docs/javadsl/KinesisSnippets.java) { #init-client }

### Kinesis as Source

The `KinesisSource` creates one `GraphStage` per shard. Reading from a shard requires an instance of `ShardSettings`.

Scala
: @@snip [snip](/kinesis/src/test/scala/docs/scaladsl/KinesisSnippets.scala) { #source-settings }

Java
: @@snip [snip](/kinesis/src/test/java/docs/javadsl/KinesisSnippets.java) { #source-settings }

You have the choice of reading from a single shard, or reading from multiple shards. In the case of multiple shards the results of running a separate `GraphStage` for each shard will be merged together.

@@@ warning
The `GraphStage` associated with a shard will remain open until the graph is stopped, or a [GetRecords](http://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetRecords.html) result returns an empty shard iterator indicating that the shard has been closed. This means that if you wish to continue processing records after a merge or reshard, you will need to recreate the source with the results of a new [DescribeStream](http://docs.aws.amazon.com/kinesis/latest/APIReference/API_DescribeStream.html) request, which can be done by simply creating a new `KinesisSource`. You can read more about adapting to a reshard in the [AWS documentation](http://docs.aws.amazon.com/streams/latest/dev/developing-consumers-with-sdk.html).
@@@

For a single shard you simply provide the settings for a single shard.

Scala
: @@snip [snip](/kinesis/src/test/scala/docs/scaladsl/KinesisSnippets.scala) { #source-single }

Java
: @@snip [snip](/kinesis/src/test/java/docs/javadsl/KinesisSnippets.java) { #source-single }

You can merge multiple shards by providing a list settings.

Scala
: @@snip [snip](/kinesis/src/test/scala/docs/scaladsl/KinesisSnippets.scala) { #source-list }

Java
: @@snip [snip](/kinesis/src/test/java/docs/javadsl/KinesisSnippets.java) { #source-list }

The constructed `Source` will return [Record](http://docs.aws.amazon.com/kinesis/latest/APIReference/API_Record.html)
objects by calling [GetRecords](http://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetRecords.html) at the specified interval and according to the downstream demand.

### Kinesis Put via Flow or as Sink

The 
@scala[@scaladoc[KinesisFlow](akka.stream.alpakka.kinesis.scaladsl.KinesisFlow$) (or @scaladoc[KinesisSink](akka.stream.alpakka.kinesis.scaladsl.KinesisSink$))] 
@java[@scaladoc[KinesisFlow](akka.stream.alpakka.kinesis.javadsl.KinesisFlow$) (or @scaladoc[KinesisSink](akka.stream.alpakka.kinesis.javadsl.KinesisSink$))] 
publishes messages into a Kinesis stream using its partition key and message body. It uses dynamic size batches, can perform several requests in parallel and retries failed records. These features are necessary to achieve the best possible write throughput to the stream. The Flow outputs the result of publishing each record.

@@@ warning
Batching has a drawback: message order cannot be guaranteed, as some records within a single batch may fail to be published. That also means that the Flow output may not match the same input order.

More information can be found in the [AWS documentation](http://docs.aws.amazon.com/streams/latest/dev/developing-producers-with-sdk.html#kinesis-using-sdk-java-putrecords) and the [AWS API reference](http://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecords.html).
@@@

In order to correlate the results with the original message, an optional user context object of arbitrary type can be associated with every message and will be returned with the corresponding result. This allows keeping track of which messages have been successfully sent to Kinesis even if the message order gets mixed up.

Publishing to a Kinesis stream requires an instance of `KinesisFlowSettings`, although a default instance with sane values and a method that returns settings based on the stream shard number are also available:

Scala
: @@snip [snip](/kinesis/src/test/scala/docs/scaladsl/KinesisSnippets.scala) { #flow-settings }

Java
: @@snip [snip](/kinesis/src/test/java/docs/javadsl/KinesisSnippets.java) { #flow-settings }

@@@ warning
Note that throughput settings `maxRecordsPerSecond` and `maxBytesPerSecond` are vital to minimize server errors (like `ProvisionedThroughputExceededException`) and retries, and thus achieve a higher publication rate.
@@@

The Flow/Sink can now be created.

Scala
: @@snip [snip](/kinesis/src/test/scala/docs/scaladsl/KinesisSnippets.scala) { #flow-sink }

Java
: @@snip [snip](/kinesis/src/test/java/docs/javadsl/KinesisSnippets.java) { #flow-sink }

## Kinesis Firehose Streams

### Create the Kinesis Firehose client

Flows provided by this connector need a `FirehoseAsyncClient` instance to publish messages.

@@@ note
The `FirehoseAsyncClient` instance you supply is thread-safe and can be shared amongst multiple `GraphStages`.
As a result, individual `GraphStages` will not automatically shutdown the supplied client when they complete.
It is recommended to shut the client instance down on Actor system termination.
@@@

Scala
: @@snip [snip](/kinesis/src/test/scala/docs/scaladsl/KinesisFirehoseSnippets.scala) { #init-client }

Java
: @@snip [snip](/kinesis/src/test/java/docs/javadsl/KinesisFirehoseSnippets.java) { #init-client }

### Kinesis Firehose Put via Flow or as Sink

The
@scala[@scaladoc[KinesisFirehoseFlow](akka.stream.alpakka.kinesisfirehose.scaladsl.KinesisFirehoseFlow$) (or @scaladoc[KinesisFirehoseSink](akka.stream.alpakka.kinesisfirehose.scaladsl.KinesisFirehoseSink$))]
@java[@scaladoc[KinesisFirehoseFlow](akka.stream.alpakka.kinesisfirehose.javadsl.KinesisFirehoseFlow$) (or @scaladoc[KinesisFirehoseSink](akka.stream.alpakka.kinesisfirehose.javadsl.KinesisFirehoseSink$))]
publishes messages into a Kinesis Firehose stream using its message body. It uses dynamic size batches, can perform several requests in parallel and retries failed records. These features are necessary to achieve the best possible write throughput to the stream. The Flow outputs the result of publishing each record.

@@@ warning
Batching has a drawback: message order cannot be guaranteed, as some records within a single batch may fail to be published. That also means that the Flow output may not match the same input order.

More information can be found in the [AWS API reference](https://docs.aws.amazon.com/firehose/latest/APIReference/API_PutRecordBatch.html).
@@@

Publishing to a Kinesis Firehose stream requires an instance of `KinesisFirehoseFlowSettings`, although a default instance with sane values is available:

Scala
: @@snip [snip](/kinesis/src/test/scala/docs/scaladsl/KinesisFirehoseSnippets.scala) { #flow-settings }

Java
: @@snip [snip](/kinesis/src/test/java/docs/javadsl/KinesisFirehoseSnippets.java) { #flow-settings }

@@@ warning
Note that throughput settings `maxRecordsPerSecond` and `maxBytesPerSecond` are vital to minimize server errors (like `ProvisionedThroughputExceededException`) and retries, and thus achieve a higher publication rate.
@@@

The Flow/Sink can now be created.

Scala
: @@snip [snip](/kinesis/src/test/scala/docs/scaladsl/KinesisFirehoseSnippets.scala) { #flow-sink }

Java
: @@snip [snip](/kinesis/src/test/java/docs/javadsl/KinesisFirehoseSnippets.java) { #flow-sink }
