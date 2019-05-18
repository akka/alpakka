# AWS SQS

@@@ note { title="Amazon Simple Queue Service" }

Amazon Simple Queue Service (Amazon SQS) offers a secure, durable, and available hosted queue that lets you integrate and decouple distributed software systems and components. Amazon SQS offers common constructs such as dead-letter queues and cost allocation tags. It provides a generic web services API and it can be accessed by any programming language that the AWS SDK supports. 

For more information about AWS SQS please visit the [official documentation](https://aws.amazon.com/documentation/sqs/).

@@@

The AWS SQS connector provides Akka Stream sources and sinks for AWS SQS queues.

@@project-info{ projectId="sqs" }


## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-sqs_$scala.binary.version$
  version=$project.version$
}

The table below shows direct dependencies of this module and the second tab shows all libraries it depends on transitively.

@@dependencies { projectId="sqs" }


## Setup

Prepare an @scaladoc[`ActorSystem`](akka.actor.ActorSystem) and a @scaladoc[`Materializer`](akka.stream.Materializer).

Scala
: @@snip [snip](/sqs/src/test/scala/akka/stream/alpakka/sqs/scaladsl/DefaultTestContext.scala) { #init-mat }

Java
: @@snip [snip](/sqs/src/test/java/akka/stream/alpakka/sqs/javadsl/BaseSqsTest.java) { #init-mat }


This connector requires an implicit @javadoc[`SqsAsyncClient`](software.amazon.awssdk.services.sqs.SqsAsyncClient) instance to communicate with AWS SQS.

It is your code's responsibility to call `close` to free any resources held by the client. In this example it will be called when the actor system is terminated.

Scala
: @@snip [snip](/sqs/src/test/scala/akka/stream/alpakka/sqs/scaladsl/DefaultTestContext.scala) { #init-client }

Java
: @@snip [snip](/sqs/src/test/java/akka/stream/alpakka/sqs/javadsl/BaseSqsTest.java) { #init-client }

### Underlying HTTP client

Alpakka SQS and SNS are set up to use @extref:[Akka HTTP](akka-http-docs:) as default HTTP client via the thin adapter library [AWS Akka-Http SPI implementation](https://github.com/matsluni/aws-spi-akka-http). By setting the `httpClient` explicitly (as above) the Akka actor system is reused, if not set explicitly a separate actor system will be created internally.

It is possible to configure the use of Netty instead, which is Amazon's default. Add an appropriate Netty version to the dependencies and configure @javadoc[`NettyNioAsyncHttpClient`](software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient).

Scala
: @@snip [snip](/sqs/src/test/scala/docs/scaladsl/SqsSourceSpec.scala) { #init-custom-client }

Java
: @@snip [snip](/sqs/src/test/java/docs/javadsl/SqsSourceTest.java) { #init-custom-client }

Please make sure to configure a big enough thread pool for the Netty client to avoid resource starvation. This is especially important,
if you share the client between multiple Sources, Sinks and Flows. For the SQS Sinks and Sources the sum of all
`parallelism` (Source) and `maxInFlight` (Sink) must be less than or equal to the thread pool size.


## Read from an SQS queue

The @scala[@scaladoc[SqsSource](akka.stream.alpakka.sqs.scaladsl.SqsSource$)]@java[@scaladoc[SqsSource](akka.stream.alpakka.sqs.javadsl.SqsSource$)] created source reads AWS Java SDK SQS `Message` objects from any SQS queue given by the queue URL.

Scala
: @@snip [snip](/sqs/src/test/scala/docs/scaladsl/SqsSourceSpec.scala) { #run }

Java
: @@snip [snip](/sqs/src/test/java/docs/javadsl/SqsSourceTest.java) { #run }

In this example we use the `closeOnEmptyReceive` to let the stream complete when there are no more messages on the queue. In realistic scenarios, you should add a `KillSwitch` to the stream, see ["Controlling stream completion with KillSwitch" in the Akka documentation](https://doc.akka.io/docs/akka/current/stream/stream-dynamic.html#controlling-stream-completion-with-killswitch).


### Source configuration

Scala
: @@snip [snip](/sqs/src/test/scala/docs/scaladsl/SqsSourceSpec.scala) { #SqsSourceSettings }

Java
: @@snip [snip](/sqs/src/test/java/docs/javadsl/SqsSourceTest.java) { #SqsSourceSettings }


Options:

 - `maxBatchSize` - the maximum number of messages to return per request (allowed values 1-10, see `MaxNumberOfMessages` in AWS docs). Default: 10
 - `maxBufferSize` - internal buffer size used by the `Source`. Default: 100 messages
 - `waitTimeSeconds` - the duration for which the call waits for a message to arrive in the queue before
    returning (see `WaitTimeSeconds` in AWS docs). Default: 20 seconds  
 - `closeOnEmptyReceive` - If true, the source completes when no messages are available.
 
More details are available in the [AWS SQS Receive Message documentation](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ReceiveMessage.html#API_ReceiveMessage_RequestParameters).
 
An `SqsSource` can either provide an infinite stream of messages (the default), or can
drain its source queue until no further messages are available. The latter
behaviour is enabled by setting the `closeOnEmptyReceive` flag on creation. If set, the
`Source` will receive messages until it encounters an empty reply from the server. It 
then continues to emit any remaining messages in its local buffer. The stage will complete
once the last message has been sent downstream.

Note that for short-polling (`waitTimeSeconds` of 0), SQS may respond with an empty 
reply even if there are still messages in the queue. This behavior can be prevented by 
switching to long-polling (by setting `waitTimeSeconds` to a nonzero value).

Be aware that the `SqsSource` runs multiple requests to Amazon SQS in parallel. The maximum number of concurrent
requests is limited by `parallelism = maxBufferSize / maxBatchSize`. E.g.: By default `maxBatchSize` is set to 10 and
`maxBufferSize` is set to 100 so at the maximum, `SqsSource` will run 10 concurrent requests to Amazon SQS. 

## Publish messages to an SQS queue

Create a `String`-accepting sink, publishing to an SQS queue.

Scala
: @@snip [snip](/sqs/src/test/scala/docs/scaladsl/SqsPublishSpec.scala) { #run-string }

Java
: @@snip [snip](/sqs/src/test/java/docs/javadsl/SqsPublishTest.java) { #run-string }


Create a `SendMessageRequest`-accepting sink, that publishes an SQS queue.

Scala
: @@snip [snip](/sqs/src/test/scala/docs/scaladsl/SqsPublishSpec.scala) { #run-send-request }

Java
: @@snip [snip](/sqs/src/test/java/docs/javadsl/SqsPublishTest.java) { #run-send-request }

You can also build flow stages which publish messages to SQS queues, backpressure on queue response, and then forward 
@scaladoc[`PublishResult`](akka.stream.alpakka.sqs.PublishResult) further down the stream.

Scala
: @@snip [snip](/sqs/src/test/scala/docs/scaladsl/SqsPublishSpec.scala) { #flow }

Java
: @@snip [snip](/sqs/src/test/java/docs/javadsl/SqsPublishTest.java) { #flow }




### Group messages and publish batches to an SQS queue

Create a sink, that forwards `String` to the SQS queue. However, the main difference from the previous use case, 
it batches items and sends as a one request and forwards a @scaladoc[`PublishResultEntry`](akka.stream.alpakka.sqs.PublishResultEntry)
further down the stream for each item processed.

Note: There is also another option to send batch of messages to SQS which is using `AmazonSQSBufferedAsyncClient`.
This client buffers `SendMessageRequest`s under the hood and sends them as a batch instead of sending them one by one. However, beware that `AmazonSQSBufferedAsyncClient`
does not support FIFO Queues. See [documentation for client-side buffering.](http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-client-side-buffering-request-batching.html)

Scala
: @@snip [snip](/sqs/src/test/scala/docs/scaladsl/SqsPublishSpec.scala) { #group }

Java
: @@snip [snip](/sqs/src/test/java/docs/javadsl/SqsPublishTest.java) { #group }


### Grouping configuration

Scala
: @@snip [snip](/sqs/src/test/scala/docs/scaladsl/SqsPublishSpec.scala) { #SqsPublishGroupedSettings }

Java
: @@snip [snip](/sqs/src/test/java/docs/javadsl/SqsPublishTest.java) { #SqsPublishGroupedSettings }


Options:

 - `maxBatchSize` - the maximum number of messages in batch to send SQS. Default: 10.
 - `maxBatchWait` - the maximum duration for which the stage waits until `maxBatchSize` messages arrived.
    Sends what is collects at the end of the time period
    even though the `maxBatchSize` is not fulfilled. Default: 500 milliseconds
 - `concurrentRequests` - the number of batches sending to SQS concurrently.


### Publish lists as batches to an SQS queue

Create a sink, that publishes @scala[`Iterable[String]`]@java[`Iterable<String>`] to the SQS queue.

Scala
: @@snip [snip](/sqs/src/test/scala/docs/scaladsl/SqsPublishSpec.scala) { #batch-string }

Java
: @@snip [snip](/sqs/src/test/java/docs/javadsl/SqsPublishTest.java) { #batch-string }

Create a sink, that publishes @scala[`Iterable[SendMessageRequest]`]@java[`Iterable<SendMessageRequest>`] to the SQS queue.

@@@ warning

Be aware that the size of the batch must be less than or equal to 10 because Amazon SQS has a limit for batch requests.
If the batch has more than 10 entries, the request will fail.

@@@

Scala
: @@snip [snip](/sqs/src/test/scala/docs/scaladsl/SqsPublishSpec.scala) { #batch-send-request }

Java
: @@snip [snip](/sqs/src/test/java/docs/javadsl/SqsPublishTest.java) { #batch-send-request }


### Batch configuration

Scala
: @@snip [snip](/sqs/src/test/scala/docs/scaladsl/SqsPublishSpec.scala) { #SqsPublishBatchSettings }

Java
: @@snip [snip](/sqs/src/test/java/docs/javadsl/SqsPublishTest.java) { #SqsPublishBatchSettings }

Options:

 - `concurrentRequests` - the number of batches sending to SQS concurrently.


## Updating message statuses

`SqsAckSink` and `SqsAckFlow` provide the possibility to acknowledge (delete), ignore, or postpone messages on an SQS queue.
They accept @scaladoc[`MessageAction`](akka.stream.alpakka.sqs.MessageAction) sub-classes to select the action to be taken.

For every message you may decide which action to take and push it together with message back to the queue:

 - `Delete` - delete message from the queue
 - `Ignore` - don't change that message, and let it reappear in the queue after the visibility timeout
 - `ChangeMessageVisibility(visibilityTimeout)` - can be used to postpone a message, or make
 the message immediately visible to other consumers. See [official documentation](http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html)
for more details.


### Acknowledge (delete) messages

Scala
: @@snip [snip](/sqs/src/test/scala/docs/scaladsl/SqsAckSpec.scala) { #ack }

Java
: @@snip [snip](/sqs/src/test/java/docs/javadsl/SqsAckTest.java) { #ack }


### Ignore messages

Scala
: @@snip [snip](/sqs/src/test/scala/docs/scaladsl/SqsAckSpec.scala) { #ignore }

Java
: @@snip [snip](/sqs/src/test/java/docs/javadsl/SqsAckTest.java) { #ignore }


### Change Visibility Timeout of messages

Scala
: @@snip [snip](/sqs/src/test/scala/docs/scaladsl/SqsAckSpec.scala) { #requeue }

Java
: @@snip [snip](/sqs/src/test/java/docs/javadsl/SqsAckTest.java) { #requeue }


### Update message status in a flow

The `SqsAckFlow` forwards a @scaladoc[`SqsAckResult`](akka.stream.alpakka.sqs.SqsAckResult) sub-class down the stream:

- `DeleteResult` to acknowledge message deletion
- `ChangeMessageVisibilityResult` to acknowledge message visibility change
- In case of `Ignore` action, nothing is performed on the sqs queue, thus no `SqsAckResult` is forwarded.

Scala
: @@snip [snip](/sqs/src/test/scala/docs/scaladsl/SqsAckSpec.scala) { #flow-ack }

Java
: @@snip [snip](/sqs/src/test/java/docs/javadsl/SqsAckTest.java) { #flow-ack }



### SqsAck configuration

Scala
: @@snip [snip](/sqs/src/test/scala/docs/scaladsl/SqsAckSpec.scala) { #SqsAckSettings }

Java
: @@snip [snip](/sqs/src/test/java/docs/javadsl/SqsAckTest.java) { #SqsAckSettings }


Options:

 - `maxInFlight` - maximum number of messages being processed by `AmazonSQSAsync` at the same time. Default: 10


### Updating message statuses in batches with grouping

`SqsAckFlow.grouped` batches actions on their type and forwards a @scaladoc[`SqsAckResultEntry`](akka.stream.alpakka.sqs.SqsAckResultEntry) 
sub-class for each item processed:

- `DeleteResultEntry` to acknowledge message deletion
- `ChangeMessageVisibilityResultEntry` to acknowledge message visibility change
- In case of `Ignore` action, nothing is performed on the sqs queue, thus no `SqsAckResult` is forwarded.

Acknowledge (delete) messages:

Scala
: @@snip [snip](/sqs/src/test/scala/docs/scaladsl/SqsAckSpec.scala) { #batch-ack }

Java
: @@snip [snip](/sqs/src/test/java/docs/javadsl/SqsAckTest.java) { #batch-ack }

Ignore messages:

Scala
: @@snip [snip](/sqs/src/test/scala/docs/scaladsl/SqsAckSpec.scala) { #batch-ignore }

Java
: @@snip [snip](/sqs/src/test/java/docs/javadsl/SqsAckTest.java) { #batch-ignore }

Change Visibility Timeout of messages:

Scala
: @@snip [snip](/sqs/src/test/scala/docs/scaladsl/SqsAckSpec.scala) { #batch-requeue }

Java
: @@snip [snip](/sqs/src/test/java/docs/javadsl/SqsAckTest.java) { #batch-requeue }


### Acknowledge grouping configuration

Scala
: @@snip [snip](/sqs/src/test/scala/docs/scaladsl/SqsAckSpec.scala) { #SqsAckGroupedSettings }

Java
: @@snip [snip](/sqs/src/test/java/docs/javadsl/SqsAckTest.java) { #SqsAckGroupedSettings }


Options:

 - `maxBatchSize` - the maximum number of messages in batch to send SQS. Default: 10.
 - `maxBatchWait` - the maximum duration for which the stage waits until `maxBatchSize` messages arrived.
    Sends what is collects at the end of the time period
    even though the `maxBatchSize` is not fulfilled. Default: 500 milliseconds
 - `concurrentRequests` - the number of batches sending to SQS concurrently.


## Integration testing

For integration testing without touching Amazon SQS, Alpakka uses [ElasticMQ](https://github.com/adamw/elasticmq), 
a queuing service which serves an AWS SQS compatible API.

### Running the example code

The code in this guide is part of runnable tests of this project. You are welcome to edit the code and run it in sbt.

> The code requires ElasticMQ running in the background. You can start it quickly using docker:
>
> `docker-compose up elasticmq`

Scala
:   ```
    sbt 'project sqs' test
    ```

Java
:   ```
    sbt 'project sqs' test
    ```
