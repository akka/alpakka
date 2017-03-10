# AWS SNS Connector

The AWS SNS connector provides an Akka Stream Flow and Sink for push notifications through AWS SNS.

For more information about AWS SNS please visit the [official documentation](https://aws.amazon.com/documentation/sns/).

## Artifacts

sbt
:   @@@vars
    ```scala
    libraryDependencies += "com.lightbend.akka" %% "akka-stream-alpakka-sns" % "$version$"
    ```
    @@@

Maven
:   @@@vars
    ```xml
    <dependency>
      <groupId>com.lightbend.akka</groupId>
      <artifactId>akka-stream-alpakka-sns_$scala.binaryVersion$</artifactId>
      <version>$version$</version>
    </dependency>
    ```
    @@@

Gradle
:   @@@vars
    ```gradle
    dependencies {
      compile group: "com.lightbend.akka", name: "akka-stream-alpakka-sns_$scala.binaryVersion$", version: "$version$"
    }
    ```
    @@@

## Usage

Sources provided by this connector need a prepared `AmazonSNSAsyncClient` to publish messages to a topic.

Scala
: @@snip (../../../../sns/src/test/scala/akka/stream/alpakka/sns/scaladsl/Examples.scala) { #init-client }

Java
: @@snip (../../../../sns/src/test/java/akka/stream/alpakka/sns/javadsl/Examples.java) { #init-client }

We will also need an @scaladoc[ActorSystem](akka.actor.ActorSystem) and an @scaladoc[ActorMaterializer](akka.stream.ActorMaterializer).

Scala
: @@snip (../../../../sns/src/test/scala/akka/stream/alpakka/sns/scaladsl/Examples.scala) { #init-system }

Java
: @@snip (../../../../sns/src/test/java/akka/stream/alpakka/sns/javadsl/Examples.java) { #init-system }

This is all preparation that we are going to need.

### Publish messages to a SNS topic

Now we can publish a String message to any SNS topic where we have access to by providing the topic ARN to the
@scaladoc[SnsPublishFlow](akka.stream.alpakka.sns.scaladsl.SnsPublishFlow$) or
@scaladoc[SnsPublishSink](akka.stream.alpakka.sns.scaladsl.SnsPublishSink$) factory.

### SnsPublishFlow

Scala
: @@snip (../../../../sns/src/test/scala/akka/stream/alpakka/sns/scaladsl/Examples.scala) { #use-flow }

Java
: @@snip (../../../../sns/src/test/java/akka/stream/alpakka/sns/javadsl/Examples.java) { #use-flow }

As you can see, this would publish the messages from the source to the specified AWS SNS topic.
After a message has been successfully published, the messageId of the published notification
will be pushed to the downstream.

### SnsPublishSink

Scala
: @@snip (../../../../sns/src/test/scala/akka/stream/alpakka/sns/scaladsl/Examples.scala) { #use-sink }

Java
: @@snip (../../../../sns/src/test/java/akka/stream/alpakka/sns/javadsl/Examples.java) { #use-sink }

As you can see, this would publish the messages from the source to the specified AWS SNS topic.

### Running the example code

The code in this guide is part of runnable tests of this project. You are welcome to edit the code and run it in sbt.

Scala
:   ```
    sbt
    > sns/test
    ```