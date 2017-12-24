# AMQP Connector

The AMQP connector provides Akka Stream sources and sinks to connect to AMQP servers.

## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-amqp_$scalaBinaryVersion$
  version=$version$
}

## Usage

### Sending messages to AMQP server

First define a queue name and the declaration of the queue that the messages will be sent to.

Scala
: @@snip ($alpakka$/amqp/src/test/scala/akka/stream/alpakka/amqp/scaladsl/AmqpConnectorsSpec.scala) { #queue-declaration }

Java
: @@snip ($alpakka$/amqp/src/test/java/akka/stream/alpakka/amqp/javadsl/AmqpConnectorsTest.java) { #queue-declaration }

Here we used @scaladoc[QueueDeclaration](akka.stream.alpakka.amqp.QueueDeclaration) configuration class to create a queue declaration. All of the configuration classes as well as connector factories are under the @scaladoc[akka.stream.alpakka.amqp](akka.stream.alpakka.amqp.package) package.

Create a sink, that accepts and forwards @scaladoc[ByteString](akka.util.ByteString)s to the AMQP server.

Scala
: @@snip ($alpakka$/amqp/src/test/scala/akka/stream/alpakka/amqp/scaladsl/AmqpConnectorsSpec.scala) { #create-sink }

Java
: @@snip ($alpakka$/amqp/src/test/java/akka/stream/alpakka/amqp/javadsl/AmqpConnectorsTest.java) { #create-sink }

@scaladoc[AmqpSink](akka.stream.alpakka.amqp.AmqpSink$) is a collection of factory methods that facilitates creation of sinks. Here we created a *simple* sink, which means that we are able to pass `ByteString`s to the sink instead of wrapping data into @scaladoc[OutgoingMessage](akka.stream.alpakka.amqp.OutgoingMessage)s.

Last step is to @extref[materialize](akka-docs:scala/stream/stream-flows-and-basics) and run the sink we have created.

Scala
: @@snip ($alpakka$/amqp/src/test/scala/akka/stream/alpakka/amqp/scaladsl/AmqpConnectorsSpec.scala) { #run-sink }

Java
: @@snip ($alpakka$/amqp/src/test/java/akka/stream/alpakka/amqp/javadsl/AmqpConnectorsTest.java) { #run-sink }

### Receiving messages from AMQP server

Create a source using the same queue declaration as before.

Scala
: @@snip ($alpakka$/amqp/src/test/scala/akka/stream/alpakka/amqp/scaladsl/AmqpConnectorsSpec.scala) { #create-source }

Java
: @@snip ($alpakka$/amqp/src/test/java/akka/stream/alpakka/amqp/javadsl/AmqpConnectorsTest.java) { #create-source }

The `bufferSize` parameter controls the maximum number of messages to prefetch from the AMQP server.

Run the source and take the same amount of messages as we previously sent to it.

Scala
: @@snip ($alpakka$/amqp/src/test/scala/akka/stream/alpakka/amqp/scaladsl/AmqpConnectorsSpec.scala) { #run-source }

Java
: @@snip ($alpakka$/amqp/src/test/java/akka/stream/alpakka/amqp/javadsl/AmqpConnectorsTest.java) { #run-source }

This is how you send and receive message from AMQP server using this connector.

### Using Pub/Sub with an AMQP server

Instead of sending messages directly to queues, it is possible to send messages to an exchange and then provide instructions to AMQP server what to do with incoming messages to the exchange. We are going to use the *fanout* type of the exchange, which enables message broadcasting to multiple consumers. We are going to do that by using an exchange declaration for the sink and all of the sources.

Scala
: @@snip ($alpakka$/amqp/src/test/scala/akka/stream/alpakka/amqp/scaladsl/AmqpConnectorsSpec.scala) { #exchange-declaration }

Java
: @@snip ($alpakka$/amqp/src/test/java/akka/stream/alpakka/amqp/javadsl/AmqpConnectorsTest.java) { #exchange-declaration }

The sink for the exchange is created in a very similar way.

Scala
: @@snip ($alpakka$/amqp/src/test/scala/akka/stream/alpakka/amqp/scaladsl/AmqpConnectorsSpec.scala) { #create-exchange-sink }

Java
: @@snip ($alpakka$/amqp/src/test/java/akka/stream/alpakka/amqp/javadsl/AmqpConnectorsTest.java) { #create-exchange-sink }

For the source, we are going to create multiple sources and merge them using @extref[Akka Streams API](akka-docs:scala/stream/stages-overview).

Scala
: @@snip ($alpakka$/amqp/src/test/scala/akka/stream/alpakka/amqp/scaladsl/AmqpConnectorsSpec.scala) { #create-exchange-source }

Java
: @@snip ($alpakka$/amqp/src/test/java/akka/stream/alpakka/amqp/javadsl/AmqpConnectorsTest.java) { #create-exchange-source }

We merge all sources into one and add the index of the source to all incoming messages, so we can distinguish which source the incoming message came from.

Such sink and source can be started the same way as in the previous example.

### Using rabbitmq as an RPC mechanism

If you have remote workers that you want to incorporate into a stream, you can do it using rabbit RPC workflow [RabbitMQ RPC](https://www.rabbitmq.com/tutorials/tutorial-six-java.html)

Scala
: @@snip ($alpakka$/amqp/src/test/scala/akka/stream/alpakka/amqp/scaladsl/AmqpConnectorsSpec.scala) { #create-rpc-flow }

Java
: @@snip ($alpakka$/amqp/src/test/java/akka/stream/alpakka/amqp/javadsl/AmqpConnectorsTest.java) { #create-rpc-flow }


Scala
: @@snip ($alpakka$/amqp/src/test/scala/akka/stream/alpakka/amqp/scaladsl/AmqpConnectorsSpec.scala) { #run-rpc-flow }

Java
: @@snip ($alpakka$/amqp/src/test/java/akka/stream/alpakka/amqp/javadsl/AmqpConnectorsTest.java) { #run-rpc-flow }


### Acknowledging messages downstream

Create a committable sink which returns 

Scala
: @@snip ($alpakka$/amqp/src/test/scala/akka/stream/alpakka/amqp/scaladsl/AmqpConnectorsSpec.scala) { #create-source-withoutautoack }

Java
: @@snip ($alpakka$/amqp/src/test/java/akka/stream/alpakka/amqp/javadsl/AmqpConnectorsTest.java) { #create-source-withoutautoack }

Committable sources return @scaladoc[CommittableIncomingMessage](akka.stream.alpakka.amqp.CommittableIncomingMessage) which wraps the @scaladoc[IncomingMessage](akka.stream.alpakka.amqp.IncomingMessage) and exposes the methods ack and nack.

Use ack to acknowledge the message back to RabbitMQ. Ack takes an optional boolean parameter `multiple` indicating whether you are acknowledging the individual message or all the messages up to it.

Scala
: @@snip ($alpakka$/amqp/src/test/scala/akka/stream/alpakka/amqp/scaladsl/AmqpConnectorsSpec.scala) { #run-source-withoutautoack }

Java
: @@snip ($alpakka$/amqp/src/test/java/akka/stream/alpakka/amqp/javadsl/AmqpConnectorsTest.java) { #run-source-withoutautoack }

Use nack to reject a message. Apart from the `multiple` argument, nack takes another optional boolean parameter indicating whether the item should be requeued or not.

Scala
: @@snip ($alpakka$/amqp/src/test/scala/akka/stream/alpakka/amqp/scaladsl/AmqpConnectorsSpec.scala) { #run-source-withoutautoack-and-nack }

Java
: @@snip ($alpakka$/amqp/src/test/java/akka/stream/alpakka/amqp/javadsl/AmqpConnectorsTest.java) { #run-source-withoutautoack-and-nack }

### Running the example code

The code in this guide is part of runnable tests of this project. You are welcome to edit the code and run it in sbt.

> Test code requires AMQP server running in the background. You can start one quickly using docker:
>
> `docker-compose up amqp`

Scala
:   ```
    sbt
    > amqp/testOnly *.AmqpConnectorsSpec
    ```

Java
:   ```
    sbt
    > amqp/testOnly *.AmqpConnectorsTest
    ```
