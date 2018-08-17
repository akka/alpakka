# AWS SQS

@@@ note { title="Amazon Simple Queue Service" }

Amazon Simple Queue Service (Amazon SQS) offers a secure, durable, and available hosted queue that lets you integrate and decouple distributed software systems and components. Amazon SQS offers common constructs such as dead-letter queues and cost allocation tags. It provides a generic web services API and it can be accessed by any programming language that the AWS SDK supports. 

For more information about AWS SQS please visit the [official documentation](https://aws.amazon.com/documentation/sqs/).

@@@

The AWS SQS connector provides Akka Stream sources and sinks for AWS SQS queues.

### Reported issues

[Tagged issues at Github](https://github.com/akka/alpakka/labels/p%3Aaws-sqs)


## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-sqs_$scala.binary.version$
  version=$project.version$
}

## Setup

Prepare an @scaladoc[ActorSystem](akka.actor.ActorSystem) and a @scaladoc[Materializer](akka.stream.Materializer).

Scala
: @@snip [snip](/sqs/src/test/scala/akka/stream/alpakka/sqs/scaladsl/DefaultTestContext.scala) { #init-mat }

Java
: @@snip [snip](/sqs/src/test/java/akka/stream/alpakka/sqs/javadsl/BaseSqsTest.java) { #init-mat }


This connector requires an implicit `AmazonSQSAsync` instance to communicate with AWS SQS. 

It is your code's responsibility to call `shutdown` to free any resources held by the client. In this example it will be called when the actor system is terminated.

Scala
: @@snip [snip](/sqs/src/test/scala/akka/stream/alpakka/sqs/scaladsl/DefaultTestContext.scala) { #init-client }

Java
: @@snip [snip](/sqs/src/test/java/akka/stream/alpakka/sqs/javadsl/BaseSqsTest.java) { #init-client }


## Read from an SQS queue

The @scala[@scaladoc[SqsSource](akka.stream.alpakka.sqs.scaladsl.SqsSource$)]@java[@scaladoc[SqsSource](akka.stream.alpakka.sqs.javadsl.SqsSource$)] created source reads AWS Java SDK SQS `Message` objects from any SQS queue given by the queue URL.

Scala
: @@snip [snip](/sqs/src/test/scala/docs/scaladsl/SqsSourceSnippetsSpec.scala) { #run }

Java
: @@snip [snip](/sqs/src/test/java/docs/javadsl/SqsSourceTest.java) { #run }

In this example we use the `closeOnEmptyReceive` to let the stream complete when there are no more messages on the queue. In realistic scenarios, you should add a `KillSwitch` to the stream, see ["Controlling stream completion with KillSwitch" in the Akka documentation](https://doc.akka.io/docs/akka/current/stream/stream-dynamic.html#controlling-stream-completion-with-killswitch).


### Source configuration

Scala
: @@snip [snip](/sqs/src/test/scala/docs/scaladsl/SqsSourceSnippetsSpec.scala) { #SqsSourceSettings }

Java
: @@snip [snip](/sqs/src/test/java/docs/javadsl/SqsSourceTest.java) { #SqsSourceSettings }


Options:

 - `maxBatchSize` - the maximum number of messages to return (see `MaxNumberOfMessages` in AWS docs). Default: 10
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
`maxBufferSize` is set to 100 so at the maximum, `SqsSource` will run 10 concurrent requests to Amazon SQS. `AmazonSQSAsyncClient`
uses a fixed thread pool with 50 threads by default. To tune the thread pool used by
`AmazonSQSAsyncClient` you can supply a custom `ExecutorService` on client creation.

Scala
: @@snip [snip](/sqs/src/test/scala/docs/scaladsl/SqsSourceSnippetsSpec.scala) { #init-custom-client }

Java
: @@snip [snip](/sqs/src/test/java/docs/javadsl/SqsSourceTest.java) { #init-custom-client }

Please make sure to configure a big enough thread pool to avoid resource starvation. This is especially important,
if you share the client between multiple Sources, Sinks and Flows. For the SQS Sinks and Sources the sum of all
`parallelism` (Source) and `maxInFlight` (Sink) must be less than or equal to the thread pool size.


## Write to an SQS queue

Create a sink, that forwards `String` to the SQS queue.

Scala
: @@snip [snip](/sqs/src/test/scala/docs/scaladsl/SinkSnippetsSpec.scala) { #run-string }

Java
: @@snip [snip](/sqs/src/test/java/docs/javadsl/SqsSinkTest.java) { #run-string }

Create a sink, that forwards `SendMessageRequest` to the SQS queue.

Scala
: @@snip [snip](/sqs/src/test/scala/docs/scaladsl/SinkSnippetsSpec.scala) { #run-send-request }

Java
: @@snip [snip](/sqs/src/test/java/docs/javadsl/SqsSinkTest.java) { #run-send-request }


## Write batches to an SQS queue

Create a sink, that forwards `String` to the SQS queue. However, the main difference from the previous use case, it batches items and sends as a one request.

Note: There is also another option to send batch of messages to SQS which is using `AmazonSQSBufferedAsyncClient`.
This client buffers `SendMessageRequest`s under the hood and sends them as a batch instead of sending them one by one. However, beware that `AmazonSQSBufferedAsyncClient`
does not support FIFO Queues. See [documentation for client-side buffering.](http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-client-side-buffering-request-batching.html)

Scala
: @@snip [snip](/sqs/src/test/scala/docs/scaladsl/SinkSnippetsSpec.scala) { #group }

Java
: @@snip [snip](/sqs/src/test/java/docs/javadsl/SqsSinkTest.java) { #group }


### Batch configuration

Scala
: @@snip [snip](/sqs/src/test/scala/docs/scaladsl/SettingsSnippetsSpec.scala) { #SqsBatchFlowSettings }

Java
: @@snip [snip](/sqs/src/test/java/docs/javadsl/SqsSinkTest.java) { #SqsBatchFlowSettings }


Options:

 - `maxBatchSize` - the maximum number of messages in batch to send SQS. Default: 10.
 - `maxBatchWait` - the maximum duration for which the stage waits until `maxBatchSize` messages arrived.
    Sends what is collects at the end of the time period
    even though the `maxBatchSize` is not fulfilled. Default: 500 milliseconds
 - `concurrentRequests` - the number of batches sending to SQS concurrently.


## Write sequences as batches to an SQS queue

Create a sink, that forwards @scala[`Seq[String]`]@java[`List<String>`] to the SQS queue.

Be aware that the size of the batch must be less than or equal to 10 because Amazon SQS has a limit for batch request.
If the batch has more than 10 entries, the request will fail.

Scala
: @@snip [snip](/sqs/src/test/scala/docs/scaladsl/SinkSnippetsSpec.scala) { #batch-string }

Java
: @@snip [snip](/sqs/src/test/java/docs/javadsl/SqsSinkTest.java) { #batch-string }

Create a sink, that forwards `Seq[SendMessageRequest]` to the SQS queue.

Be aware that the size of the batch must be less than or equal to 10 because Amazon SQS has a limit for batch request.
If the batch has more than 10 entries, the request will fail.

Scala
: @@snip [snip](/sqs/src/test/scala/docs/scaladsl/SinkSnippetsSpec.scala) { #batch-send-request }

Java
: @@snip [snip](/sqs/src/test/java/docs/javadsl/SqsSinkTest.java) { #batch-send-request }


### Sink configuration

Scala
: @@snip [snip](/sqs/src/test/scala/docs/scaladsl/SettingsSnippetsSpec.scala) { #SqsSinkSettings }

Java
: @@snip [snip](/sqs/src/test/java/docs/javadsl/SqsSinkTest.java) { #SqsSinkSettings }

Options:

 - `maxInFlight` - maximum number of messages being processed by `AmazonSQSAsync` at the same time. Default: 10


## Message processing with acknowledgement

`SqsAckSink` provides possibility to acknowledge (delete), ignore, or postpone a message.

Your flow must decide which action to take and push it with message:

 - `Delete` - delete message from the queue
 - `Ignore` - ignore the message and let it reappear in the queue after visibility timeout
 - `ChangeMessageVisibility(visibilityTimeout: Int)` - can be used to postpone a message, or make
 the message immediately visible to other consumers. See [official documentation](http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html)
for more details.

Acknowledge (delete) messages:

Scala
: @@snip [snip](/sqs/src/test/scala/docs/scaladsl/SinkSnippetsSpec.scala) { #ack }

Java
: @@snip [snip](/sqs/src/test/java/docs/javadsl/SqsAckSinkTest.java) { #ack }

Ignore messages:

Scala
: @@snip [snip](/sqs/src/test/scala/docs/scaladsl/SinkSnippetsSpec.scala) { #ignore }

Java
: @@snip [snip](/sqs/src/test/java/docs/javadsl/SqsAckSinkTest.java) { #ignore }

Change Visibility Timeout of messages:

Scala
: @@snip [snip](/sqs/src/test/scala/docs/scaladsl/SinkSnippetsSpec.scala) { #requeue }

Java
: @@snip [snip](/sqs/src/test/java/docs/javadsl/SqsAckSinkTest.java) { #requeue }

### SqsAckSink configuration

Same as the normal `SqsSink`:

Scala
: @@snip [snip](/sqs/src/test/scala/docs/scaladsl/SettingsSnippetsSpec.scala) { #SqsAckSinkSettings }

Java
: @@snip [snip](/sqs/src/test/java/docs/javadsl/SqsSinkTest.java) { #SqsAckSinkSettings }


Options:

 - `maxInFlight` - maximum number of messages being processed by `AmazonSQSAsync` at the same time. Default: 10


## Message processing with acknowledgement with batching

`SqsAckFlow.grouped` is a flow that can acknowledge (delete), ignore, or postpone messages, but it batches items and sends them as one request per action.

Acknowledge (delete) messages:

Scala
: @@snip [snip](/sqs/src/test/scala/docs/scaladsl/FlowSnippetsSpec.scala) { #batch-ack }

Java
: @@snip [snip](/sqs/src/test/java/docs/javadsl/SqsAckSinkTest.java) { #batch-ack }

Ignore messages:

Scala
: @@snip [snip](/sqs/src/test/scala/docs/scaladsl/FlowSnippetsSpec.scala) { #batch-ignore }

Java
: @@snip [snip](/sqs/src/test/java/docs/javadsl/SqsAckSinkTest.java) { #batch-ignore }

Change Visibility Timeout of messages:

Scala
: @@snip [snip](/sqs/src/test/scala/docs/scaladsl/FlowSnippetsSpec.scala) { #batch-requeue }

Java
: @@snip [snip](/sqs/src/test/java/docs/javadsl/SqsAckSinkTest.java) { #batch-requeue }


### Batch configuration

Scala
: @@snip [snip](/sqs/src/test/scala/docs/scaladsl/SettingsSnippetsSpec.scala) { #SqsBatchAckFlowSettings }

Java
: @@snip [snip](/sqs/src/test/java/docs/javadsl/SqsSinkTest.java) { #SqsBatchAckFlowSettings }


Options:

 - `maxBatchSize` - the maximum number of messages in batch to send SQS. Default: 10.
 - `maxBatchWait` - the maximum duration for which the stage waits until `maxBatchSize` messages arrived.
    Sends what is collects at the end of the time period
    even though the `maxBatchSize` is not fulfilled. Default: 500 milliseconds
 - `concurrentRequests` - the number of batches sending to SQS concurrently.


## Using SQS as a Flow

You can also build flow stages which put or acknowledge messages in SQS, backpressure on queue response and then forward
responses further down the stream. The API is similar to creating Sinks.

Scala
: @@snip [snip](/sqs/src/test/scala/docs/scaladsl/FlowSnippetsSpec.scala) { #flow }

Java
: @@snip [snip](/sqs/src/test/java/docs/javadsl/SqsSinkTest.java) { #flow }


With Ack:

Scala
: @@snip [snip](/sqs/src/test/scala/docs/scaladsl/FlowSnippetsSpec.scala) { #flow-ack }

Java
: @@snip [snip](/sqs/src/test/java/docs/javadsl/SqsAckSinkTest.java) { #flow-ack }


### Running the example code

The code in this guide is part of runnable tests of this project. You are welcome to edit the code and run it in sbt.

> The test code uses embedded [ElasticMQ](https://github.com/adamw/elasticmq) as queuing service which serves an AWS SQS
> compatible API.

Scala
:   ```
    sbt 'project sqs' test
    ```

Java
:   ```
    sbt 'project sqs' test
    ```
