# AWS SQS Connector

The AWS SQS connector provides Akka Stream sources and sinks for AWS SQS queues.

For more information about AWS SQS please visit the [official documentation](https://aws.amazon.com/documentation/sqs/).

## Artifacts

sbt
:   @@@vars
    ```scala
    libraryDependencies += "com.lightbend.akka" %% "akka-stream-alpakka-sqs" % "$version$"
    ```
    @@@

Maven
:   @@@vars
    ```xml
    <dependency>
      <groupId>com.lightbend.akka</groupId>
      <artifactId>akka-stream-alpakka-sqs_$scalaBinaryVersion$</artifactId>
      <version>$version$</version>
    </dependency>
    ```
    @@@

Gradle
:   @@@vars
    ```gradle
    dependencies {
      compile group: "com.lightbend.akka", name: "akka-stream-alpakka-sqs_$scalaBinaryVersion$", version: "$version$"
    }
    ```
    @@@

## Usage

Sources, Flows and Sinks provided by this connector need a prepared `AmazonSQSAsync` to load messages from a queue.

Scala
: @@snip (../../../../sqs/src/test/scala/akka/stream/alpakka/sqs/scaladsl/DefaultTestContext.scala) { #init-client }

Java
: @@snip (../../../../sqs/src/test/java/akka/stream/alpakka/sqs/javadsl/BaseSqsTest.java) { #init-client }

We will also need an @scaladoc[ActorSystem](akka.actor.ActorSystem) and an @scaladoc[ActorMaterializer](akka.stream.ActorMaterializer).

Scala
: @@snip (../../../../sqs/src/test/scala/akka/stream/alpakka/sqs/scaladsl/DefaultTestContext.scala) { #init-mat }

Java
: @@snip (../../../../sqs/src/test/java/akka/stream/alpakka/sqs/javadsl/SqsSourceTest.java) { #init-mat }

This is all preparation that we are going to need.

### Stream messages from a SQS queue

Now we can stream AWS Java SDK SQS `Message` objects from any SQS queue where we have access to by providing the queue URL to the
@scaladoc[SqsSource](akka.stream.alpakka.sqs.scaladsl.SqsSource$) factory.

Scala
: @@snip (../../../../sqs/src/test/scala/akka/stream/alpakka/sqs/scaladsl/SqsSourceSpec.scala) { #run }

Java
: @@snip (../../../../sqs/src/test/java/akka/stream/alpakka/sqs/javadsl/SqsSourceTest.java) { #run }

As you have seen we take the first 100 elements from the stream. The reason for this is, that reading messages from
SQS queues never finishes because there is no direct way to determine the end of a queue.

#### Source configuration

Scala
: @@snip (../../../../sqs/src/main/scala/akka/stream/alpakka/sqs/SqsSourceSettings.scala) { #SqsSourceSettings }

Options:

 - `maxBatchSize` - the maximum number of messages to return (see `MaxNumberOfMessages` in AWS docs). Default: 10
 - `maxBufferSize` - internal buffer size used by the `Source`. Default: 100 messages
 - `waitTimeSeconds` - the duration for which the call waits for a message to arrive in the queue before
    returning (see `WaitTimeSeconds` in AWS docs). Default: 20 seconds

Be aware that the `SqsSource` runs multiple requests to Amazon SQS in parallel. The maximum number of concurrent
requests is limited by `parallelism = maxBufferSize / maxBatchSize`. E.g.: By default `maxBatchSize` is set to 10 and
`maxBufferSize` is set to 100 so at the maximum, `SqsSource` will run 10 concurrent requests to Amazon SQS. `AmazonSQSAsyncClient`
uses a fixed thread pool with 50 threads by default. To tune the thread pool used by
`AmazonSQSAsyncClient` you can supply a custom `ExecutorService` on client creation.

Scala
: @@snip (../../../../sqs/src/test/scala/akka/stream/alpakka/sqs/scaladsl/SqsSourceSpec.scala) { #init-custom-client }

Java
: @@snip (../../../../sqs/src/test/java/akka/stream/alpakka/sqs/javadsl/SqsSourceTest.java) { #init-custom-client }

Please make sure to configure a big enough thread pool to avoid resource starvation. This is especially important
if you share the client between multiple Sources, Sinks and Flows. For the SQS Sinks and Sources the sum of all
`parallelism` (Source) and `maxInFlight` (Sink) must be less than or equal to the thread pool size.

### Stream messages to a SQS queue

Create a sink, that forwards `String` to the SQS queue.

Scala
: @@snip (../../../../sqs/src/test/scala/akka/stream/alpakka/sqs/scaladsl/SqsSpec.scala) { #run }

Java
: @@snip (../../../../sqs/src/test/java/akka/stream/alpakka/sqs/javadsl/SqsSinkTest.java) { #run }

#### Sink configuration

Scala
: @@snip (../../../../sqs/src/main/scala/akka/stream/alpakka/sqs/SqsSinkSettings.scala) { #SqsSinkSettings }

Options:

 - `maxInFlight` - maximum number of messages being processed by `AmazonSQSAsync` at the same time. Default: 10


### Message processing with acknowledgement

`SqsAckSink` provides possibility to acknowledge (delete) or requeue a message.

Your flow must decide which action to take and push it with message:

 - `Ack` - delete message from the queue
 - `RequeueWithDelay(delaySeconds: Int)` - schedule a retry
 
Scala (ack)
: @@snip (../../../../sqs/src/test/scala/akka/stream/alpakka/sqs/scaladsl/SqsSpec.scala) { #ack }

Scala (requeue)
: @@snip (../../../../sqs/src/test/scala/akka/stream/alpakka/sqs/scaladsl/SqsSpec.scala) { #requeue }

Java (ack)
: @@snip (../../../../sqs/src/test/java/akka/stream/alpakka/sqs/javadsl/SqsAckSinkTest.java) { #ack }

Java (requeue)
: @@snip (../../../../sqs/src/test/java/akka/stream/alpakka/sqs/javadsl/SqsAckSinkTest.java) { #requeue }

#### SqsAckSink configuration

Same as the normal `SqsSink`:

Scala
: @@snip (../../../../sqs/src/main/scala/akka/stream/alpakka/sqs/SqsAckSinkSettings.scala) { #SqsAckSinkSettings }

Options:

 - `maxInFlight` - maximum number of messages being processed by `AmazonSQSAsync` at the same time. Default: 10

### Using SQS as a Flow

You can also build flow stages which put or acknowledge messages in SQS, backpressure on queue response and then forward
responses further down the stream. The API is similar to creating Sinks.

Scala (flow)
: @@snip (../../../../sqs/src/test/scala/akka/stream/alpakka/sqs/scaladsl/SqsSpec.scala) { #flow }

Java (flow)
: @@snip (../../../../sqs/src/test/java/akka/stream/alpakka/sqs/javadsl/SqsSinkTest.java) { #flow }

Scala (flow with ack)
: @@snip (../../../../sqs/src/test/scala/akka/stream/alpakka/sqs/scaladsl/SqsSpec.scala) { #flow-ack }

Java (flow with ack)
: @@snip (../../../../sqs/src/test/java/akka/stream/alpakka/sqs/javadsl/SqsAckSinkTest.java) { #flow-ack }

### Running the example code

The code in this guide is part of runnable tests of this project. You are welcome to edit the code and run it in sbt.

> The test code uses embedded [ElasticMQ](https://github.com/adamw/elasticmq) as queuing service which serves an AWS SQS
> compatible API.

Scala
:   ```
    sbt
    > sqs/testOnly *.SqsSourceSpec
    ```

Java
:   ```
    sbt
    > sqs/testOnly *.SqsSourceTest
    ```
