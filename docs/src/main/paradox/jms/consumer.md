# Consumer

The Alpakka JMS connector offers consuming JMS messages from topics or queues:

* Read `javax.jms.Message`s from an Akka Streams source 
* Allow for client acknowledgement to the JMS broker 
* Allow for JMS transactions 
* Read raw JVM types from an Akka Streams Source

The JMS message model supports several types of message bodies in (see @javadoc[javax.jms.Message](javax.jms.Message)), which may be created directly from the Akka Stream elements, or in wrappers to access more advanced features.


## Receiving messages

@java[@scaladoc[JmsConsumer](akka.stream.alpakka.jms.javadsl.JmsConsumer$)]@scala[@scaladoc[JmsConsumer](akka.stream.alpakka.jms.scaladsl.JmsConsumer$)] offers factory methods to consume JMS messages in a number of ways.

This examples shows how to listen to a JMS queue and emit @javadoc[javax.jms.Message](javax.jms.Message) elements into the stream.

The materialized value `JmsConsumerControl` is used to shut down the consumer (it is a @scaladoc[Killswitch](akka.stream.KillSwitch)) and offers the possibility to inspect the connectivity state of the consumer. 

Scala
: @@snip [snip](/jms/src/test/scala/docs/scaladsl/JmsConnectorsSpec.scala) { #jms-source }

Java
: @@snip [snip](/jms/src/test/java/docs/javadsl/JmsConnectorsTest.java) { #jms-source }


## Configure JMS consumers

To connect to the JMS broker, first define an appropriate @javadoc[javax.jms.ConnectionFactory](javax.jms.ConnectionFactory). The Alpakka tests and all examples use Active MQ.

Scala
: @@snip [snip](/jms/src/test/scala/docs/scaladsl/JmsConnectorsSpec.scala) { #connection-factory }

Java
: @@snip [snip](/jms/src/test/java/docs/javadsl/JmsConnectorsTest.java) { #connection-factory }


The created @javadoc[ConnectionFactory](javax.jms.ConnectionFactory) is then used for the creation of the different JMS sources.

The `JmsConsumerSettings` factories allow for passing the actor system to read from the default `alpakka.jms.consumer` section, or you may pass a `Config` instance which is resolved to a section of the same structure. 

Scala
: @@snip [snip](/jms/src/test/scala/docs/scaladsl/JmsSettingsSpec.scala) { #consumer-settings }

Java
: @@snip [snip](/jms/src/test/java/docs/javadsl/JmsSettingsTest.java) { #consumer-settings }

The Alpakka JMS consumer is configured via default settings in the [HOCON](https://github.com/lightbend/config#using-hocon-the-json-superset) config file section `alpakka.jms.consumer` in your `application.conf`, and settings may be tweaked in the code using the `withXyz` methods. On the second tab the section from `reference.conf` shows the structure to use for configuring multiple set-ups.

Table
: Setting               | Description                                                          | Default Value       | 
------------------------|----------------------------------------------------------------------|---------------------|
connectionFactory       | Factory to use for creating JMS connections                          | Must be set in code |
destination             | Destination (queue or topic) to send JMS messages to                 | Must be set in code |
credentials             | JMS broker credentials                                               | Empty               |
connectionRetrySettings | Retry characteristics if the connection failed to be established or is taking a long time. | See @ref[Connection Retries](producer.md#connection-retries) 
sessionCount            | Number of parallel sessions to use for receiving JMS messages.       | defaults to `1`     |
bufferSize              | Maximum number of messages to prefetch before applying backpressure. | 100                 |
ackTimeout              | For use with JMS transactions, only: maximum time given to a message to be committed or rolled back. | 1 second  |
selector                | JMS selector expression (see [below](#using-jms-selectors))          | Empty               |
connectionStatusSubscriptionTimeout | 5 seconds | Time to wait for subscriber of connection status events before starting to discard them |

reference.conf
: @@snip [snip](/jms/src/main/resources/reference.conf) { #consumer }


### Broker specific destinations

To reach out to special features of the JMS broker, destinations can be created as `CustomDestination` which takes a factory method for creating destinations.

Scala
: @@snip [snip](/jms/src/test/scala/docs/scaladsl/JmsConnectorsSpec.scala) { #custom-destination }

Java
: @@snip [snip](/jms/src/test/java/docs/javadsl/JmsConnectorsTest.java) { #custom-destination }


## Using JMS client acknowledgement

Client acknowledgement ensures a message is successfully received by the consumer and notifies the JMS broker for every message. Due to the threading details in JMS brokers, this special source is required (see the explanation below).

Scala
: @@snip [snip](/jms/src/test/scala/docs/scaladsl/JmsBufferedAckConnectorsSpec.scala) { #source }

Java
: @@snip [snip](/jms/src/test/java/docs/javadsl/JmsBufferedAckConnectorsTest.java) { #source }

The `sessionCount` parameter controls the number of JMS sessions to run in parallel.


**Notes:**

*  Using multiple sessions increases throughput, especially if acknowledging message by message is desired.
*  Messages may arrive out of order if `sessionCount` is larger than 1.
*  Message-by-message acknowledgement can be achieved by setting `bufferSize` to 0, thus disabling buffering. The outstanding messages before backpressure will be the `sessionCount`.
*  The default `AcknowledgeMode` is `ClientAcknowledge` but can be overridden to custom `AcknowledgeMode`s, even implementation-specific ones by setting the `AcknowledgeMode` in the `JmsConsumerSettings` when creating the stream.

@@@ warning

Using a regular `JmsConsumer`  with `AcknowledgeMode.ClientAcknowledge` and using `message.acknowledge()` from the stream is not compliant with the JMS specification and can cause issues for some message brokers. `message.acknowledge()` in many cases acknowledges the session and not the message itself, contrary to what the API makes you believe.

Use this `JmsConsumer.ackSource` as shown above instead.

@@@


## Using JMS transactions

JMS transactions may be used with this connector. Be aware that transactions are a heavy-weight tool and may not perform very good.

Scala
: @@snip [snip](/jms/src/test/scala/docs/scaladsl/JmsTxConnectorsSpec.scala) { #source }

Java
: @@snip [snip](/jms/src/test/java/docs/javadsl/JmsTxConnectorsTest.java) { #source }

The `sessionCount` parameter controls the number of JMS sessions to run in parallel.

The `ackTimeout` parameter controls the maximum time given to a message to be committed or rolled back. If the message times out it will automatically be rolled back. This is to prevent stream from starvation if the application fails to commit or rollback a message, or if the message errors out and the stream is resumed by a `decider`.

**Notes:**

*  Higher throughput is achieved by increasing the `sessionCount`.
*  Messages will arrive out of order if `sessionCount` is larger than 1.
*  Buffering is not supported in transaction mode. The `bufferSize` is ignored.
*  The default `AcknowledgeMode` is `SessionTransacted` but can be overridden to custom `AcknowledgeMode`s, even implementation-specific ones by setting the `AcknowledgeMode` in the `JmsConsumerSettings` when creating the stream.



## Using JMS selectors

Create a @javadoc[javax.jms.Message](javax.jms.Message) source specifying a [JMS selector expression](https://docs.oracle.com/cd/E19798-01/821-1841/bncer/index.html):
Verify that we are only receiving messages according to the selector:

Scala
: @@snip [snip](/jms/src/test/scala/docs/scaladsl/JmsConnectorsSpec.scala) { #source-with-selector }

Java
: @@snip [snip](/jms/src/test/java/docs/javadsl/JmsConnectorsTest.java) { #source-with-selector }


## Raw JVM type sources

| Stream element type                                   | Alpakka source factory   |
|-------------------------------------------------------|--------------------------|
| String                                                | [`JmsConsumer.textSource`](#text-sources)   |
| @scala[Array[Byte]]@java[byte[]]                      | [`JmsConsumer.bytesSource`](#byte-array-sources)  |
| @scala[Map[String, AnyRef]]@java[Map<String, Object>] | [`JmsConsumer.mapSource`](#map-messages-sources)  |
| Object (`java.io.Serializable`)                       | [`JmsConsumer.objectSource`](#object-sources)     |

### Text sources

The `textSource` emits the received message body as String:

Scala
: @@snip [snip](/jms/src/test/scala/docs/scaladsl/JmsConnectorsSpec.scala) { #text-source }

Java
: @@snip [snip](/jms/src/test/java/docs/javadsl/JmsConnectorsTest.java) { #text-source }


### Byte array sources

The `bytesSource` emits the received message body as byte array:

Scala
: @@snip [snip](/jms/src/test/scala/docs/scaladsl/JmsConnectorsSpec.scala) { #bytearray-source }

Java
: @@snip [snip](/jms/src/test/java/docs/javadsl/JmsConnectorsTest.java) { #bytearray-source }


### Map sources

The `mapSource` emits the received message body as @scala[Map[String, Object]]@java[Map<String, Object>]:

Scala
: @@snip [snip](/jms/src/test/scala/docs/scaladsl/JmsConnectorsSpec.scala) { #map-source }

Java
: @@snip [snip](/jms/src/test/java/docs/javadsl/JmsConnectorsTest.java) { #map-source }


### Object sources

The `objectSource` emits the received message body as deserialized JVM instance. As serialization may be a security concern, JMS clients require special configuration to allow this. The example shows how to configure ActiveMQ connection factory to support serialization. See [ActiveMQ Security](http://activemq.apache.org/objectmessage.html) for more information on this.

Scala
: @@snip [snip](/jms/src/test/scala/docs/scaladsl/JmsConnectorsSpec.scala) { #object-source }

Java
: @@snip [snip](/jms/src/test/java/docs/javadsl/JmsConnectorsTest.java) { #object-source }


## Request / Reply

The request / reply pattern can be implemented by streaming a @java[@scaladoc[JmsConsumer](akka.stream.alpakka.jms.javadsl.JmsConsumer$)]@scala[@scaladoc[JmsConsumer](akka.stream.alpakka.jms.scaladsl.JmsConsumer$)]
to a @java[@scaladoc[JmsProducer](akka.stream.alpakka.jms.javadsl.JmsProducer$)]@scala[@scaladoc[JmsProducer](akka.stream.alpakka.jms.scaladsl.JmsProducer$)],
with a stage in between that extracts the `ReplyTo` and `CorrelationID` from the original message and adds them to the response.

Scala
: @@snip [snip](/jms/src/test/scala/docs/scaladsl/JmsConnectorsSpec.scala) { #request-reply }

Java
: @@snip [snip](/jms/src/test/java/docs/javadsl/JmsConnectorsTest.java) { #request-reply }
