# AWS Kinesis Connector

The AWS Kinesis connector provides an Akka Stream Source for consuming Kinesis Stream records.

For more information about Kinesis please visit the [official documentation](https://aws.amazon.com/documentation/kinesis/).

## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-kinesis$scalaBinaryVersion$
  version=$version$
}

## Usage

Sources and Flows provided by this connector need a `AmazonKinesisAsync` instance to consume messages from a shard.

@@@ note
The `AmazonKinesisAsync` instance you supply is thread-safe and can be shared amongst multiple `GraphStages`. As a result, individual `GraphStages` will not automatically shutdown the supplied client when they complete.
@@@

Scala
: @@snip ($alpakka$/kinesis/src/test/scala/akka/stream/alpakka/kinesis/scaladsl/Examples.scala) { #init-client }

Java
: @@snip ($alpakka$/kinesis/src/test/java/akka/stream/alpakka/kinesis/javadsl/Examples.java) { #init-client }

We will also need an @scaladoc[ActorSystem](akka.actor.ActorSystem) and an @scaladoc[ActorMaterializer](akka.stream.ActorMaterializer).

Scala
: @@snip ($alpakka$/kinesis/src/test/scala/akka/stream/alpakka/kinesis/scaladsl/Examples.scala) { #init-system }

Java
: @@snip ($alpakka$/kinesis/src/test/java/akka/stream/alpakka/kinesis/javadsl/Examples.java) { #init-system }

### Using the Source

The `KinesisSource` creates one `GraphStage` per shard. Reading from a shard requires an instance of `ShardSettings`.

Scala
: @@snip ($alpakka$/kinesis/src/test/scala/akka/stream/alpakka/kinesis/scaladsl/Examples.scala) { #source-settings }

Java
: @@snip ($alpakka$/kinesis/src/test/java/akka/stream/alpakka/kinesis/javadsl/Examples.java) { #source-settings }

You have the choice of reading from a single shard, or reading from multiple shards. In the case of multiple shards the results of running a separate `GraphStage` for each shard will be merged together.

@@@ warning
The `GraphStage` associated with a shard will remain open until the graph is stopped, or a [GetRecords](http://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetRecords.html) result returns an empty shard iterator indicating that the shard has been closed. This means that if you wish to continue processing records after a merge or reshard, you will need to recreate the source with the results of a new [DescribeStream](http://docs.aws.amazon.com/kinesis/latest/APIReference/API_DescribeStream.html) request, which can be done by simply creating a new `KinesisSource`. You can read more about adapting to a reshard [here](http://docs.aws.amazon.com/streams/latest/dev/developing-consumers-with-sdk.html).
@@@

For a single shard you simply provide the settings for a single shard.

Scala
: @@snip ($alpakka$/kinesis/src/test/scala/akka/stream/alpakka/kinesis/scaladsl/Examples.scala) { #source-single }

Java
: @@snip ($alpakka$/kinesis/src/test/java/akka/stream/alpakka/kinesis/javadsl/Examples.java) { #source-single }

You can merge multiple shards by providing a list settings.

Scala
: @@snip ($alpakka$/kinesis/src/test/scala/akka/stream/alpakka/kinesis/scaladsl/Examples.scala) { #source-list }

Java
: @@snip ($alpakka$/kinesis/src/test/java/akka/stream/alpakka/kinesis/javadsl/Examples.java) { #source-list }

The constructed `Source` will return [Record](http://docs.aws.amazon.com/kinesis/latest/APIReference/API_Record.html)

objects by calling [GetRecords](http://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetRecords.html) at the specified interval and according to the downstream demand.

### Using the Put Flow/Sink

The `KinesisFlow` (or `KinesisSink`) publishes messages into a Kinesis stream using it's partition key and message body. It uses dynamic size batches, can perform several requests in parallel and retries failed records. These features are necessary to achieve the best possible write throughput to the stream. The Flow outputs the result of publishing each record.

@@@ warning
Batching has a drawback: message order cannot be guaranteed, as some records within a single batch may fail to be published. That also means that the Flow output may not match the same input order.

More information can be found [here](http://docs.aws.amazon.com/streams/latest/dev/developing-producers-with-sdk.html#kinesis-using-sdk-java-putrecords) and [here](http://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecords.html).
@@@

Publishing to a Kinesis stream requires an instance of `KinesisFlowSettings`, although a default instance with sane values and a method that returns settings based on the stream shard number are also available:

Scala
: @@snip ($alpakka$/kinesis/src/test/scala/akka/stream/alpakka/kinesis/scaladsl/Examples.scala) { #flow-settings }

Java
: @@snip ($alpakka$/kinesis/src/test/java/akka/stream/alpakka/kinesis/javadsl/Examples.java) { #flow-settings }

@@@ warning
Note that throughput settings `maxRecordsPerSecond` and `maxBytesPerSecond` are vital to minimize server errors (like `ProvisionedThroughputExceededException`) and retries, and thus achieve a higher publication rate.
@@@

The Flow/Sink can now be created.

Scala
: @@snip ($alpakka$/kinesis/src/test/scala/akka/stream/alpakka/kinesis/scaladsl/Examples.scala) { #flow-sink }

Java
: @@snip ($alpakka$/kinesis/src/test/java/akka/stream/alpakka/kinesis/javadsl/Examples.java) { #flow-sink }

