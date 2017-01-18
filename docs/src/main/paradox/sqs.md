# AWS SQS Connector

The AWS SQS connector provides Akka Stream sources and sinks for AWS SQS queues.

For more information about AWS SQS please visit the [official docmentation](https://aws.amazon.com/documentation/sqs/).

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
      <artifactId>akka-stream-alpakka-sqs_$scala.binaryVersion$</artifactId>
      <version>$version$</version>
    </dependency>
    ```
    @@@

Gradle
:   @@@vars
    ```gradle
    dependencies {
      compile group: "com.lightbend.akka", name: "akka-stream-alpakka-sqs_$scala.binaryVersion$", version: "$version$"
    }
    ```
    @@@

## Usage

Sources provided by this connector need a prepared `AmazonSQSAsyncClient` to load messages from a queue.

Scala
: @@snip (../../../../sqs/src/test/scala/akka/stream/alpakka/sqs/scaladsl/DefaultTestContext.scala) { #init-client }

Java
: @@snip (../../../../sqs/src/test/java/akka/stream/alpakka/sqs/javadsl/SqsSourceTest.java) { #init-client }

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
: @@snip (../../../../sqs/src/main/scala/akka/stream/alpakka/sqs/SqsSourceStage.scala) { #SqsSourceSettings }

Options:

 - `maxBatchSize` - the maximum number of messages to return (see `MaxNumberOfMessages` in AWS docs). Default: 10
 - `maxBufferSize` - internal buffer size used by the `Source`. Default: 100 messages
 - `waitTimeSeconds` - the duration for which the call waits for a message to arrive in the queue before
    returning (see `WaitTimeSeconds` in AWS docs). Default: 20 seconds

### Stream messages to a SQS queue

Create a sink, that forwards `String` to the SQS queue.

Scala
: @@snip (../../../../sqs/src/test/scala/akka/stream/alpakka/sqs/scaladsl/SqsSpec.scala) { #run }

Java
: @@snip (../../../../sqs/src/test/java/akka/stream/alpakka/sqs/javadsl/SqsSinkTest.java) { #run }


#### Sink configuration

Scala
: @@snip (../../../../sqs/src/main/scala/akka/stream/alpakka/sqs/SqsSinkStage.scala) { #SqsSinkSettings }

Options:

 - `maxInFlight` - maximum number of messages being processed by `AmazonSQSAsync` at the same time. Default: 10

### Running the example code

The code in this guide is part of runnable tests of this project. You are welcome to edit the code and run it in sbt.

> The test code uses [ElasticMQ](https://github.com/adamw/elasticmq) as queuing service which serves an AWS SQS
> compatible API.  You can start one quickly using docker:
>
> `docker run -p 9324:9324 -d s12v/elasticmq`

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
