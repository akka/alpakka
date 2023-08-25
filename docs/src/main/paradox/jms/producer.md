# Producer

The Alpakka JMS connector offers producing JMS messages to topics or queues in three ways

* JVM types to an Akka Streams Sink
* `JmsMessage` sub-types to a Akka Streams Sink or Flow (using `JmsProducer.sink` or `JmsProducer.flow`)
* `JmsEnvelope` sub-types to a Akka Streams Flow (using `JmsProducer.flexiFlow`) to support pass-throughs

The JMS message model supports several types of message bodies in (see @javadoc[javax.jms.Message](javax.jms.Message)), which may be created directly from the Akka Stream elements, or in wrappers to access more advanced features.

| Stream element type                                       | Alpakka producer         |
|-----------------------------------------------------------|--------------------------|
| `String`                                                  | [`JmsProducer.textSink`](#text-sinks)   |
| @scala[`Array[Byte]`]@java[`byte[]`]                      | [`JmsProducer.bytesSink`](#byte-array-sinks)  |
| @scala[`Map[String, AnyRef]`]@java[`Map<String, Object>`] | [`JmsProducer.mapSink`](#map-messages-sink)  |
| `Object` (`java.io.Serializable`)                         | [`JmsProducer.objectSink`](#object-sinks)     |
| `JmsTextMessage`                                          | [`JmsProducer.sink`](#a-jmsmessage-sub-type-sink) or [`JmsProducer.flow`](#sending-messages-as-a-flow) |
| `JmsByteMessage`                                          | [`JmsProducer.sink`](#a-jmsmessage-sub-type-sink) or [`JmsProducer.flow`](#sending-messages-as-a-flow) |
| `JmsByteStringMessage`                                    | [`JmsProducer.sink`](#a-jmsmessage-sub-type-sink) or [`JmsProducer.flow`](#sending-messages-as-a-flow) |
| `JmsMapMessage`                                           | [`JmsProducer.sink`](#a-jmsmessage-sub-type-sink) or [`JmsProducer.flow`](#sending-messages-as-a-flow) |
| `JmsObjectMessage`                                        | [`JmsProducer.sink`](#a-jmsmessage-sub-type-sink) or [`JmsProducer.flow`](#sending-messages-as-a-flow) |
| @scala[`JmsEnvelope[PassThrough]`]@java[`JmsEnvelope<PassThrough>`] with instances `JmsPassThrough`, `JmsTextMessagePassThrough`, `JmsByteMessagePassThrough`, `JmsByteStringMessagePassThrough`, `JmsMapMessagePassThrough`, `JmsObjectMessagePassThrough`      | [`JmsProducer.flexiFlow`](#passing-context-through-the-producer) |



### Configure JMS producers

To connect to the JMS broker, first define an appropriate @javadoc[javax.jms.ConnectionFactory](javax.jms.ConnectionFactory). Here we're using Active MQ.

Scala
: @@snip [snip](/jms/src/test/scala/docs/scaladsl/JmsConnectorsSpec.scala) { #connection-factory }

Java
: @@snip [snip](/jms/src/test/java/docs/javadsl/JmsConnectorsTest.java) { #connection-factory }


The created @javadoc[ConnectionFactory](javax.jms.ConnectionFactory) is then used for the creation of the different JMS sinks or sources (see below).

### A `JmsMessage` sub-type sink

Use a case class with the subtype of @apidoc[JmsMessage] to wrap the messages you want to send and optionally set message specific properties or headers.
@apidoc[JmsProducer$] contains factory methods to facilitate the creation of sinks according to the message type.

Scala
: @@snip [snip](/jms/src/test/scala/docs/scaladsl/JmsConnectorsSpec.scala) { #create-jms-sink }

Java
: @@snip [snip](/jms/src/test/java/docs/javadsl/JmsConnectorsTest.java) { #create-jms-sink }


#### Setting JMS message properties

For every @apidoc[JmsMessage] you can set JMS message properties.

Scala
: @@snip [snip](/jms/src/test/scala/docs/scaladsl/JmsConnectorsSpec.scala) { #create-messages-with-properties }

Java
: @@snip [snip](/jms/src/test/java/docs/javadsl/JmsConnectorsTest.java) { #create-messages-with-properties }


#### Setting JMS message header attributes
For every @apidoc[JmsMessage] you can set also JMS message headers.

Scala
: @@snip [snip](/jms/src/test/scala/docs/scaladsl/JmsConnectorsSpec.scala) { #create-messages-with-headers }

Java
: @@snip [snip](/jms/src/test/java/docs/javadsl/JmsConnectorsTest.java) { #create-messages-with-headers }


### Raw JVM type sinks

#### Text sinks

Create a sink, that accepts and forwards @apidoc[JmsTextMessage$]s to the JMS provider:

Scala
: @@snip [snip](/jms/src/test/scala/docs/scaladsl/JmsConnectorsSpec.scala) { #text-sink }

Java
: @@snip [snip](/jms/src/test/java/docs/javadsl/JmsConnectorsTest.java) { #text-sink }

#### Byte array sinks

Create a sink, that accepts and forwards @apidoc[JmsByteMessage$]s to the JMS provider.

Scala
: @@snip [snip](/jms/src/test/scala/docs/scaladsl/JmsConnectorsSpec.scala) { #bytearray-sink }

Java
: @@snip [snip](/jms/src/test/java/docs/javadsl/JmsConnectorsTest.java) { #bytearray-sink  }


#### Map message sink

Create a sink, that accepts and forwards @apidoc[JmsMapMessage$]s to the JMS provider:

Scala
: @@snip [snip](/jms/src/test/scala/docs/scaladsl/JmsConnectorsSpec.scala) { #map-sink }

Java
: @@snip [snip](/jms/src/test/java/docs/javadsl/JmsConnectorsTest.java) { #map-sink }


#### Object sinks

Create and configure ActiveMQ connection factory to support serialization.
See [ActiveMQ Security](https://activemq.apache.org/objectmessage.html) for more information on this.
Create a sink, that accepts and forwards @apidoc[JmsObjectMessage$]s to the JMS provider:

Scala
: @@snip [snip](/jms/src/test/scala/docs/scaladsl/JmsConnectorsSpec.scala) { #object-sink }

Java
: @@snip [snip](/jms/src/test/java/docs/javadsl/JmsConnectorsTest.java) { #object-sink }


### Sending messages as a Flow

The producer can also act as a flow, in order to publish messages in the middle of stream processing.
For example, you can ensure that a message is persisted to the queue before subsequent processing.


Scala
: @@snip [snip](/jms/src/test/scala/docs/scaladsl/JmsConnectorsSpec.scala) { #flow-producer }

Java
: @@snip [snip](/jms/src/test/java/docs/javadsl/JmsConnectorsTest.java) { #flow-producer }


### Sending messages with per-message destinations

It is also possible to define message destinations per message:

Scala
: @@snip [snip](/jms/src/test/scala/docs/scaladsl/JmsConnectorsSpec.scala) { #run-directed-flow-producer }

Java
: @@snip [snip](/jms/src/test/java/docs/javadsl/JmsConnectorsTest.java) { #run-directed-flow-producer }

When no destination is defined on the message, the destination given in the producer settings is used.


### Passing context through the producer

In some use cases, it is useful to pass through context information when producing (e.g. for acknowledging or committing
messages after sending to Jms). For this, the `JmsProducer.flexiFlow` accepts implementations of `JmsEnvelope`,
which it will pass through:

* `JmsPassThrough`
* `JmsTextMessagePassThrough`
* `JmsByteMessagePassThrough`
* `JmsByteStringMessagePassThrough` 
* `JmsMapMessagePassThrough`
* `JmsObjectMessagePassThrough`  

Scala
: @@snip [snip](/jms/src/test/scala/docs/scaladsl/JmsConnectorsSpec.scala) { #run-flexi-flow-producer }

Java
: @@snip [snip](/jms/src/test/java/docs/javadsl/JmsConnectorsTest.java) { #run-flexi-flow-producer }

There are two implementations: One envelope type containing a messages to send to Jms, and one
envelope type containing only values to pass through. This allows messages to flow without producing any new messages 
to Jms. This is primarily useful when committing offsets back to Kakfa, or when acknowledging Jms messages after sending
the outcome of processing them back to Jms.

Scala
: @@snip [snip](/jms/src/test/scala/docs/scaladsl/JmsConnectorsSpec.scala) { #run-flexi-flow-pass-through-producer }

Java
: @@snip [snip](/jms/src/test/java/docs/javadsl/JmsConnectorsTest.java) { #run-flexi-flow-pass-through-producer }


## Producer Settings

The Alpakka JMS producer is configured via default settings in the [HOCON](https://github.com/lightbend/config#using-hocon-the-json-superset) config file section `alpakka.jms.producer` in your `application.conf`, and settings may be tweaked in the code using the `withXyz` methods.

The `JmsProducerSettings` factories allow for passing the actor system to read from the default  `alpakka.jms.producer` section, or you may pass a `Config` instance which is resolved to a section of the same structure. 

Scala
: @@snip [snip](/jms/src/test/scala/docs/scaladsl/JmsSettingsSpec.scala) { #producer-settings }

Java
: @@snip [snip](/jms/src/test/java/docs/javadsl/JmsSettingsTest.java) { #producer-settings }

The producer can be configured with the following settings. On the second tab, the section from `reference.conf` shows the structure to use for configuring multiple set-ups.

Table
: Setting                 | Defaults    |   Description                                           | 
--------------------------|-------------|---------------------------------------------------------|
connectionFactory         | mandatory   | Factory to use for creating JMS connections             |
destination               | mandatory   | Destination (queue or topic) to send JMS messages to    |
credentials               | optional    | JMS broker credentials                                  |
connectionRetrySettings   | default settings | Retry characteristics if the connection failed to be established or taking a long time. Please see default values under [Connection Retries](#connection-retries) |
sendRetrySettings         | default settings | Retry characteristics if message sending failed. Please see default values under [Send Retries](#send-retries) |
sessionCount              | defaults to `1` | Number of parallel sessions to use for sending JMS messages. Increasing the number of parallel sessions increases throughput at the cost of message ordering. While the messages may arrive out of order on the JMS broker, the producer flow outputs messages in the order they are received |
timeToLive                | optional    | Time messages should be kept on the Jms broker. This setting can be overridden on individual messages. If not set, messages will never expire |
connectionStatusSubscriptionTimeout | 5 seconds | Time to wait for subscriber of connection status events before starting to discard them |

reference.conf
: @@snip [snip](/jms/src/main/resources/reference.conf) { #producer }


## Connection Retries

When a connection to a broker cannot be established and errors out, or is timing out being established or started, the connection can be retried. All JMS publishers, consumers, and browsers are configured with connection retry settings. On the second tab the section from `reference.conf` shows the structure to use for configuring multiple set-ups.

Table
: Setting        | Description                                                                         | Default Value
---------------|-------------------------------------------------------------------------------------|--------------
connectTimeout | Time allowed to establish and start a connection                                    | 10 s
initialRetry   | Wait time before retrying the first time                                            | 100 ms
backoffFactor  | Back-off factor for subsequent retries                                              | 2.0
maxBackoff     | Maximum back-off time allowed, after which all retries will happen after this delay | 1 minute
maxRetries     | Maximum number of retries allowed (negative value is infinite)                      | 10

reference.conf
: @@snip [snip](/jms/src/main/resources/reference.conf) { #connection-retry }


The retry time is calculated by:

*initialRetry \* retryNumber<sup>backoffFactor</sup>*

With the default settings, we'll see retries after 100ms, 400ms, 900ms pauses, until the pauses reach 1 minute and will stay with 1 minute intervals for any subsequent retries.

Consumers, producers and browsers try to reconnect with the same retry characteristics if a connection fails mid-stream.

All JMS settings support setting the `connectionRetrySettings` field using `.withConnectionRetrySettings(retrySettings)` on the given settings. The followings show how to create `ConnectionRetrySettings`:

Scala
: @@snip [snip](/jms/src/test/scala/docs/scaladsl/JmsSettingsSpec.scala) { #retry-settings-case-class }

Java
: @@snip [snip](/jms/src/test/java/docs/javadsl/JmsSettingsTest.java) { #retry-settings }

## Send Retries

When a connection to a broker starts failing, sending JMS messages will also fail. Those failed messages can be retried
at the cost of potentially duplicating the failed messages. Send retries can be configured as follows:

Table
: Setting        | Description                                                                         | Default Value
---------------|-------------------------------------------------------------------------------------|--------------
initialRetry   | Wait time before retrying the first time                                            | 20 ms
backoffFactor  | Back-off factor for subsequent retries                                              | 1.5
maxBackoff     | Maximum back-off time allowed, after which all retries will happen after this delay | 500 ms
maxRetries     | Maximum number of retries allowed (negative value is infinite)                      | 10

reference.conf
: @@snip [snip](/jms/src/main/resources/reference.conf) { #send-retry }

The retry time is calculated by:

*initialRetry \* retryNumber<sup>backoffFactor</sup>*

With the default settings, we'll see retries after 20ms, 57ms, 104ms pauses, until the pauses reach 500 ms and will stay with 500 ms intervals for any subsequent retries.

JMS producer settings support configuring retries by using `.withSendRetrySettings(retrySettings)`. The followings show how to create `SendRetrySettings`:

Scala
: @@snip [snip](/jms/src/test/scala/docs/scaladsl/JmsSettingsSpec.scala) { #send-retry-settings }

Java
: @@snip [snip](/jms/src/test/java/docs/javadsl/JmsSettingsTest.java) { #send-retry-settings }

If a send operation finally fails, the stage also fails unless a different supervision strategy is applied. The 
producer stage honours stream supervision.


### Observing connectivity and state of a JMS producer

All JMS producer's materialized values are of type `JmsProducerStatus`. This provides a `connectorState` method returning
a `Source` of `JmsConnectorState` updates that publishes connection attempts, disconnections, completions and failures.
The source is completed after the JMS producer completes or fails.

