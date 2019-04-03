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


@@@warning { title="API may change" }

The Alpakka SQS API may change as it could support [SQS FiFo queues with little effort](https://github.com/akka/alpakka/pull/1604). That and a few other possible enhancements let us open up for API changes within the 1.0.x releases of Alpakka.

See [Alpakka SQS issues](https://github.com/akka/alpakka/labels/p%3Aaws-sqs)

@@@

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
: @@snip [snip](/sqs/src/test/scala/docs/scaladsl/SqsSourceSpec.scala) { #init-custom-client }

Java
: @@snip [snip](/sqs/src/test/java/docs/javadsl/SqsSourceTest.java) { #init-custom-client }

Please make sure to configure a big enough thread pool to avoid resource starvation. This is especially important,
if you share the client between multiple Sources, Sinks and Flows. For the SQS Sinks and Sources the sum of all
`parallelism` (Source) and `maxInFlight` (Sink) must be less than or equal to the thread pool size.


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

You can also build flow stages which publish messages to SQS queues, backpressure on queue response, and then forward @scaladoc[`SqsPublishResult`](akka.stream.alpakka.sqs.SqsPublishResult) further down the stream. 

Scala
: @@snip [snip](/sqs/src/test/scala/docs/scaladsl/SqsPublishSpec.scala) { #flow }

Java
: @@snip [snip](/sqs/src/test/java/docs/javadsl/SqsPublishTest.java) { #flow }




### Group messages and publish batches to an SQS queue

Create a sink, that forwards `String` to the SQS queue. However, the main difference from the previous use case, it batches items and sends as a one request.

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

Be aware that the size of the batch must be less than or equal to 10 because Amazon SQS has a limit for batch request.
If the batch has more than 10 entries, the request will fail.

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

`SqsAckSink` and `SqsAckFlow` provide the possibility to acknowledge (delete), ignore, or postpone messages on an SQS queue. They accept @scaladoc[`MessageAction`](akka.stream.alpakka.sqs.MessageAction) sub-classes to select the action to be taken.

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

The flow accepts a @scaladoc[`MessageAction`](akka.stream.alpakka.sqs.MessageAction) sub-classes, and returns @scaladoc[`SqsAckResult`](akka.stream.alpakka.sqs.SqsAckResult) .

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

`SqsAckFlow.grouped` is a flow that can acknowledge (delete), ignore, or postpone messages, but it batches items and sends them as one request per action.

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
