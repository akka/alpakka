# Consumer

## Receiving messages from a JMS provider

@java[@scaladoc[JmsConsumer](akka.stream.alpakka.jms.javadsl.JmsConsumer$)]@scala[@scaladoc[JmsConsumer](akka.stream.alpakka.jms.scaladsl.JmsConsumer$)] contains factory methods to facilitate
the creation of sinks according to the message type (see below for an example).


### Receiving @javadoc[String](java.lang.String) messages from a JMS provider

Create a source:

Scala
: @@snip [snip](/jms/src/test/scala/docs/scaladsl/JmsConnectorsSpec.scala) { #create-text-source }

Java
: @@snip [snip](/jms/src/test/java/docs/javadsl/JmsConnectorsTest.java) { #create-text-source }

The `bufferSize` parameter controls the maximum number of messages to prefetch before applying backpressure.

Run the source and take the same amount of messages as we previously sent to it.

Scala
: @@snip [snip](/jms/src/test/scala/docs/scaladsl/JmsConnectorsSpec.scala) { #run-text-source }

Java
: @@snip [snip](/jms/src/test/java/docs/javadsl/JmsConnectorsTest.java) { #run-text-source }


### Receiving byte array messages from a JMS provider

Create a source:

Scala
: @@snip [snip](/jms/src/test/scala/docs/scaladsl/JmsConnectorsSpec.scala) { #create-bytearray-source }

Java
: @@snip [snip](/jms/src/test/java/docs/javadsl/JmsConnectorsTest.java) { #create-bytearray-source }

The `bufferSize` parameter controls the maximum number of messages to prefetch before applying backpressure.

Run the source and take the same amount of messages as we previously sent to it.

Scala
: @@snip [snip](/jms/src/test/scala/docs/scaladsl/JmsConnectorsSpec.scala) { #run-bytearray-source }

Java
: @@snip [snip](/jms/src/test/java/docs/javadsl/JmsConnectorsTest.java) { #run-bytearray-source }


### Receiving @javadoc[Serializable](java.io.Serializable) object messages from a JMS provider

Create and configure ActiveMQ connection factory to support serialization.
See [ActiveMQ Security](http://activemq.apache.org/objectmessage.html) for more information on this.

Scala
: @@snip [snip](/jms/src/test/scala/docs/scaladsl/JmsConnectorsSpec.scala) { #connection-factory-object }

Java
: @@snip [snip](/jms/src/test/java/docs/javadsl/JmsConnectorsTest.java) { #connection-factory-object }


Create a source:

Scala
: @@snip [snip](/jms/src/test/scala/docs/scaladsl/JmsConnectorsSpec.scala) { #create-object-source }

Java
: @@snip [snip](/jms/src/test/java/docs/javadsl/JmsConnectorsTest.java) { #create-object-source }

The `bufferSize` parameter controls the maximum number of messages to prefetch before applying backpressure.

Run the source and take the same amount of messages as we previously sent to it.

Scala
: @@snip [snip](/jms/src/test/scala/docs/scaladsl/JmsConnectorsSpec.scala) { #run-object-source }

Java
: @@snip [snip](/jms/src/test/java/docs/javadsl/JmsConnectorsTest.java) { #run-object-source }


### Receiving @javadoc[Map](java.util.Map) messages from a JMS provider

Create a source:

Scala
: @@snip [snip](/jms/src/test/scala/docs/scaladsl/JmsConnectorsSpec.scala) { #create-map-source }

Java
: @@snip [snip](/jms/src/test/java/docs/javadsl/JmsConnectorsTest.java) { #create-map-source }

The `bufferSize` parameter controls the maximum number of messages to prefetch before applying backpressure.

Run the source and take the same amount of messages as we previously sent to it.

Scala
: @@snip [snip](/jms/src/test/scala/docs/scaladsl/JmsConnectorsSpec.scala) { #run-map-source }

Java
: @@snip [snip](/jms/src/test/java/docs/javadsl/JmsConnectorsTest.java) { #run-map-source }


### Receiving @javadoc[javax.jms.Message](javax.jms.Message)s from a JMS provider

Create a source:

Scala
: @@snip [snip](/jms/src/test/scala/docs/scaladsl/JmsConnectorsSpec.scala) { #create-jms-source }

Java
: @@snip [snip](/jms/src/test/java/docs/javadsl/JmsConnectorsTest.java) { #create-jms-source }

The `bufferSize` parameter controls the maximum number of messages to prefetch before applying backpressure.

Run the source and specify the amount of messages to take:

Scala
: @@snip [snip](/jms/src/test/scala/docs/scaladsl/JmsConnectorsSpec.scala) { #run-jms-source }

Java
: @@snip [snip](/jms/src/test/java/docs/javadsl/JmsConnectorsTest.java) { #run-jms-source }

**Notes:**

*  The default `AcknowledgeMode` is `AutoAcknowledge` but can be overridden to custom `AcknowledgeMode`s, even implementation-specific ones by setting the `AcknowledgeMode` in the `JmsConsumerSettings` when creating the stream.

### Receiving @javadoc[javax.jms.Message](javax.jms.Message)s messages from a JMS provider with Client Acknowledgement

Create a @javadoc[javax.jms.Message](javax.jms.Message) source:

Scala
: @@snip [snip](/jms/src/test/scala/docs/scaladsl/JmsConnectorsSpec.scala) { #create-jms-source-client-ack }

Java
: @@snip [snip](/jms/src/test/java/docs/javadsl/JmsConnectorsTest.java) { #create-jms-source-client-ack }

The `acknowledgeMode` (@scaladoc[AcknowledgeMode](akka.stream.alpakka.jms.AcknowledgeMode$)) parameter controls the JMS acknowledge mode parameter, see @javadoc[javax.jms.Connection.createSession](javax.jms.Connection#createSession-boolean-int-).

Run the source and take the same amount of messages as we previously sent to it acknowledging them.

Scala
: @@snip [snip](/jms/src/test/scala/docs/scaladsl/JmsConnectorsSpec.scala) { #run-jms-source-with-ack }

Java
: @@snip [snip](/jms/src/test/java/docs/javadsl/JmsConnectorsTest.java) { #run-jms-source-with-ack }


### Receiving @javadoc[javax.jms.Message](javax.jms.Message)s from a JMS provider with a selector

Create a @javadoc[javax.jms.Message](javax.jms.Message) source specifying a [JMS selector expression](https://docs.oracle.com/cd/E19798-01/821-1841/bncer/index.html):

Scala
: @@snip [snip](/jms/src/test/scala/docs/scaladsl/JmsConnectorsSpec.scala) {             #create-jms-source-with-selector }

Java
: @@snip [snip](/jms/src/test/java/docs/javadsl/JmsConnectorsTest.java) { #create-jms-source-with-selector }

Verify that we are only receiving messages according to the selector:

Scala
: @@snip [snip](/jms/src/test/scala/docs/scaladsl/JmsConnectorsSpec.scala) { #assert-only-odd-messages-received }

Java
: @@snip [snip](/jms/src/test/java/docs/javadsl/JmsConnectorsTest.java) { #assert-only-odd-messages-received }

### Receiving and explicitly acknowledging @javadoc[javax.jms.Message](javax.jms.Message)s from a JMS provider

Create a @javadoc[javax.jms.Message](javax.jms.Message) source:

Scala
: @@snip [snip](/jms/src/test/scala/docs/scaladsl/JmsBufferedAckConnectorsSpec.scala) { #create-jms-source }

Java
: @@snip [snip](/jms/src/test/java/docs/javadsl/JmsBufferedAckConnectorsTest.java) { #create-jms-source }

The `sessionCount` parameter controls the number of JMS sessions to run in parallel.

The `bufferSize` parameter controls the maximum number of messages each JMS session will prefetch and awaiting acknowledgement before applying backpressure.

Run the source and specify the amount of messages to take:

Scala
: @@snip [snip](/jms/src/test/scala/docs/scaladsl/JmsBufferedAckConnectorsSpec.scala) { #run-jms-source }

Java
: @@snip [snip](/jms/src/test/java/docs/javadsl/JmsBufferedAckConnectorsTest.java) { #run-jms-source }

**Notes:**

*  Using multiple sessions increases throughput, especially if a acknowledging message by message is desired.
*  Messages may arrive out of order if `sessionCount` is larger than 1.
*  Message-by-message acknowledgement can be achieved by setting `bufferSize` to 0, thus disabling buffering. The outstanding messages before backpressure will be the `sessionCount`.
*  The default `AcknowledgeMode` is `ClientAcknowledge` but can be overridden to custom `AcknowledgeMode`s, even implementation-specific ones by setting the `AcknowledgeMode` in the `JmsConsumerSettings` when creating the stream.

### Transactionally receiving @javadoc[javax.jms.Message](javax.jms.Message)s from a JMS provider

Create a @javadoc[javax.jms.Message](javax.jms.Message) source:

Scala
: @@snip [snip](/jms/src/test/scala/docs/scaladsl/JmsTxConnectorsSpec.scala) { #create-jms-source }

Java
: @@snip [snip](/jms/src/test/java/docs/javadsl/JmsTxConnectorsTest.java) { #create-jms-source }

The `sessionCount` parameter controls the number of JMS sessions to run in parallel.

The `ackTimeout` parameter controls the maximum time given to a message to be committed or rolled back. If the message times out it will automatically be rolled back. This is to prevent stream from starvation if the application fails to commit or rollback a message, or if the message errors out and the stream is resumed by a `decider`.

Run the source and specify the amount of messages to take:

Scala
: @@snip [snip](/jms/src/test/scala/docs/scaladsl/JmsTxConnectorsSpec.scala) { #run-jms-source }

Java
: @@snip [snip](/jms/src/test/java/docs/javadsl/JmsTxConnectorsTest.java) { #run-jms-source }

**Notes:**

*  Higher throughput is achieved by increasing the `sessionCount`.
*  Messages will arrive out of order if `sessionCount` is larger than 1.
*  Buffering is not supported in transaction mode. The `bufferSize` is ignored.
*  The default `AcknowledgeMode` is `SessionTransacted` but can be overridden to custom `AcknowledgeMode`s, even implementation-specific ones by setting the `AcknowledgeMode` in the `JmsConsumerSettings` when creating the stream.
