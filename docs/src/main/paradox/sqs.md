# AWS SQS Connector

The AWS SQS connector allows to stream SQS `Message` from a AWS SQS queue.
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
: @@snip (../../../../sqs/src/test/scala/akka/stream/alpakka/sqs/scaladsl/SqsSourceSpec.scala) { #init-client }

Java
: @@snip (../../../../sqs/src/test/java/akka/stream/alpakka/sqs/javadsl/SqsSourceTest.java) { #init-client }

We will also need an @scaladoc[ActorSystem](akka.actor.ActorSystem) and an @scaladoc[ActorMaterializer](akka.stream.ActorMaterializer).

Scala
: @@snip (../../../../sqs/src/test/scala/akka/stream/alpakka/sqs/scaladsl/SqsSourceSpec.scala) { #init-mat }

Java
: @@snip (../../../../sqs/src/test/java/akka/stream/alpakka/sqs/javadsl/SqsSourceTest.java) { #init-mat }

This is all preparation that we are going to need.

Now we can stream AWS Java SDK SQS `Message` objects from any SQS queue where we have access to by providing the queue URL to the
@scaladoc[SqsSource](akka.stream.alpakka.sqs.scaladsl.SqsSource$) factory.

Scala
: @@snip (../../../../sqs/src/test/scala/akka/stream/alpakka/sqs/scaladsl/SqsSourceSpec.scala) { #run }

Java
: @@snip (../../../../sqs/src/test/java/akka/stream/alpakka/sqs/javadsl/SqsSourceTest.java) { #run }

As you have seen we take the first 100 elements from the stream. The reason for this is, that reading messages from
SQS queues never finishes because there is no direct way to determine the end of a queue.

### Running the example code

The code in this guide is part of runnable tests of this project. You are welcome to edit the code and run it in sbt.

> The test code uses [ElasticMQ](https://github.com/adamw/elasticmq) as queuing service which serves an AWS SQS 
> compatible API.  You can start one quickly using docker:
>
> `docker run -p 9324:9324 -d expert360/elasticmq`

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
