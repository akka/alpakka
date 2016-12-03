# IronMq Connector

The IronMq connector provides an Akka stream source and sink to connect to the [IronMQ](https://www.iron.io/platform/ironmq/) queue.

## Artifacts

sbt
:   @@@vars
    ```scala
    libraryDependencies += "com.lightbend.akka" %% "akka-stream-alpakka-ironmq" % "$version$"
    ```
    @@@

Maven
:   @@@vars
    ```xml
    <dependency>
      <groupId>com.lightbend.akka</groupId>
      <artifactId>akka-stream-alpakka-ironmq_$scala.binaryVersion$</artifactId>
      <version>$version$</version>
    </dependency>
    ```
    @@@

Gradle
:   @@@vars
    ```gradle
    dependencies {
      compile group: "com.lightbend.akka", name: "akka-stream-alpakka-ironmq_$scala.binaryVersion$", version: "$version$"
    }
    ```
    @@@

## Usage

IronMq can be used either in cloud or on-premise. Either way you need a authentication token and a project ID. These can
be set in the typesafe config:

: @@snip (../../../../ironmq/src/resources/reference.conf)

### Producer

The producer could be instantiated using the @scaladoc[IronMqProducer](akka.stream.alpakka.ironmq.scaladsl.IronMqProducer).
It provides methods to obtain either a `Flow[PushMessage, Messages.Id, NotUsed]` or a `Sink[PushMessage, NotUsed]`. 

It is possible to pass an already existing @scaladoc[IronMqClient](akka.stream.alpakka.ironmq.IronMqClient) or a provider. 
In the latter case, the Stage will close the client on completion. You will need a different producer for each queue, but
a client can be shared across all the producers and consumers on the same project ID.

The @scaladoc[PushMessage](akka.stream.alpakka.ironmq.PushMessage) allow to specify the delay per individual message. The
message expiration is set a queue level. The @scaladoc[IronMqClient](akka.stream.alpakka.ironmq.IronMqClient) does not 
still have all the features to create and update these settings on a queue.

If you are using the producer `Flow` The returned @scaladoc[Messages.Ids](akka.stream.alpakka.ironmq.Messages.Id) will 
contain the ID of the pushed message, that can be used to manipulate the message.

For each `PushMessage` from the upstream you will have exact one `Message.Id` in downstream in the same order. Regardless
if the producer will implement a batch mechanism in the future.

### Consumer

At this time only a at-most-once guarantee consumer is supported (the messages are deleted straight away on consumption). 

The consumer is pull one. It will pull every `n` milliseconds, waiting for `m` milliseconds to consume new messages and
will push them to the downstream. These parameters are not configurable at the moment.

The consumer could be instantiated using the @scaladoc[IronMqConsumer](akka.stream.alpakka.ironmq.scaladsl.IronMqConsumer). 
It provides methods to obtain either a `Source[Message, NotUsed]`. You will need a different consumer for each queue, but
a client can be shared across for all the producers and consumers on the same project ID.

It is possible to pass an already existing @scaladoc[IronMqClient](akka.stream.alpakka.ironmq.IronMqClient) or a provider. 
In the latter case, the Stage will close the client on completion.