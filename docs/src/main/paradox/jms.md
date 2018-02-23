# JMS Connector

The JMS connector provides Akka Stream sources and sinks to connect to JMS providers.


### Reported issues

[Tagged issues at Github](https://github.com/akka/alpakka/labels/p%3Ajms)


## Artifacts

sbt
:   @@@vars
    ```scala
    libraryDependencies += "com.lightbend.akka" %% "akka-stream-alpakka-jms" % "$version$"
    libraryDependencies += "javax.jms" % "jms" % "1.1"
    ```
    @@@

Maven
:   @@@vars
    ```xml
    <dependency>
      <groupId>com.lightbend.akka</groupId>
      <artifactId>akka-stream-alpakka-jms_$scalaBinaryVersion$</artifactId>
      <version>$version$</version>
    </dependency>
    <dependency>
      <groupId>javax.jms</groupId>
      <artifactId>jms</artifactId>
      <version>1.1</version>
    </dependency>
    ```
    @@@

Gradle
:   @@@vars
    ```gradle
    dependencies {
      compile group: "com.lightbend.akka", name: "akka-stream-alpakka-jms_$scalaBinaryVersion$", version: "$version$"
      compile group: 'javax.jms', name: 'jms', version: '1.1'
    }
    ```
    @@@

## Usage

The JMS message model supports several types of message body (see @javadoc[javax.jms.Message](javax.jms.Message)) and Alpakka currently supports messages with a body containing a @javadoc[String](java.lang.String), 
a @javadoc[Serializable](java.io.Serializable) object, a @javadoc[Map](java.util.Map) or a byte array.

First define a @javadoc[javax.jms.ConnectionFactory](javax.jms.ConnectionFactory) depending on the implementation you're using. Here we're using Active MQ.

Scala
: @@snip ($alpakka$/jms/src/test/scala/akka/stream/alpakka/jms/scaladsl/JmsConnectorsSpec.scala) { #connection-factory }

Java
: @@snip ($alpakka$/jms/src/test/java/akka/stream/alpakka/jms/javadsl/JmsConnectorsTest.java) { #connection-factory }

The created @javadoc[ConnectionFactory](javax.jms.ConnectionFactory) is then used for the creation of the different jms sinks or sources (see below).


## Sending messages to a JMS provider

Use a case class with the subtype of @scaladoc[JmsMessage](akka.stream.alpakka.jms.JmsMessage$) to wrap the messages you want to send and optionally set their properties.
@java[@scaladoc[JmsProducer](akka.stream.alpakka.jms.javadsl.JmsProducer$)]@scala[@scaladoc[JmsProducer](akka.stream.alpakka.jms.scaladsl.JmsProducer$)] contains factory methods to facilitate
the creation of sinks according to the message type (see below for an example).


### Sending text messages to a JMS provider

Create a sink, that accepts and forwards @scaladoc[JmsTextMessage](akka.stream.alpakka.jms.JmsTextMessage$)s to the JMS provider:

Scala
: @@snip ($alpakka$/jms/src/test/scala/akka/stream/alpakka/jms/scaladsl/JmsConnectorsSpec.scala) { #create-text-sink }

Java
: @@snip ($alpakka$/jms/src/test/java/akka/stream/alpakka/jms/javadsl/JmsConnectorsTest.java) { #create-text-sink }

Last step is to @extref[materialize](akka-docs:scala/stream/stream-flows-and-basics) and run the sink(s) we have created.

Scala
: @@snip ($alpakka$/jms/src/test/scala/akka/stream/alpakka/jms/scaladsl/JmsConnectorsSpec.scala) { #run-text-sink }

Java
: @@snip ($alpakka$/jms/src/test/java/akka/stream/alpakka/jms/javadsl/JmsConnectorsTest.java) { #run-text-sink }

### Sending byte messages to a JMS provider

Create a sink, that accepts and forwards @scaladoc[JmsByteMessage](akka.stream.alpakka.jms.JmsByteMessage$)s to the JMS provider. 

Scala
: @@snip ($alpakka$/jms/src/test/scala/akka/stream/alpakka/jms/scaladsl/JmsConnectorsSpec.scala) { #create-bytearray-sink }

Java
: @@snip ($alpakka$/jms/src/test/java/akka/stream/alpakka/jms/javadsl/JmsConnectorsTest.java) { #create-bytearray-sink  }

Last step is to @extref[materialize](akka-docs:scala/stream/stream-flows-and-basics) and run the sink(s) we have created.

Scala
: @@snip ($alpakka$/jms/src/test/scala/akka/stream/alpakka/jms/scaladsl/JmsConnectorsSpec.scala) { #run-bytearray-sink }

Java
: @@snip ($alpakka$/jms/src/test/java/akka/stream/alpakka/jms/javadsl/JmsConnectorsTest.java) { #run-bytearray-sink }


### Sending map messages to a JMS provider

Create a sink, that accepts and forwards @scaladoc[JmsMapMessage](akka.stream.alpakka.jms.JmsMapMessage$)s to the JMS provider:

Scala
: @@snip ($alpakka$/jms/src/test/scala/akka/stream/alpakka/jms/scaladsl/JmsConnectorsSpec.scala) { #create-map-sink }

Java
: @@snip ($alpakka$/jms/src/test/java/akka/stream/alpakka/jms/javadsl/JmsConnectorsTest.java) { #create-map-sink }

Last step is to @extref[materialize](akka-docs:scala/stream/stream-flows-and-basics) and run the sink(s) we have created.

Scala
: @@snip ($alpakka$/jms/src/test/scala/akka/stream/alpakka/jms/scaladsl/JmsConnectorsSpec.scala) { #run-map-sink }

Java
: @@snip ($alpakka$/jms/src/test/java/akka/stream/alpakka/jms/javadsl/JmsConnectorsTest.java) { #run-map-sink }


### Sending object messages to a JMS provider

Create and configure ActiveMQ connection factory to support serialization.
See [ActiveMQ Security](http://activemq.apache.org/objectmessage.html) for more information on this.

Scala
: @@snip ($alpakka$/jms/src/test/scala/akka/stream/alpakka/jms/scaladsl/JmsConnectorsSpec.scala) { #connection-factory-object }

Java
: @@snip ($alpakka$/jms/src/test/java/akka/stream/alpakka/jms/javadsl/JmsConnectorsTest.java) { #connection-factory-object }


Create a sink, that accepts and forwards @scaladoc[JmsObjectMessage](akka.stream.alpakka.jms.JmsObjectMessage$)s to the JMS provider:

Scala
: @@snip ($alpakka$/jms/src/test/scala/akka/stream/alpakka/jms/scaladsl/JmsConnectorsSpec.scala) { #create-object-sink }

Java
: @@snip ($alpakka$/jms/src/test/java/akka/stream/alpakka/jms/javadsl/JmsConnectorsTest.java) { #create-object-sink }


Last step is to @extref[materialize](akka-docs:scala/stream/stream-flows-and-basics) and run the sink(s) we have created.

Scala
: @@snip ($alpakka$/jms/src/test/scala/akka/stream/alpakka/jms/scaladsl/JmsConnectorsSpec.scala) { #run-object-sink }

Java
: @@snip ($alpakka$/jms/src/test/java/akka/stream/alpakka/jms/javadsl/JmsConnectorsTest.java) { #run-object-sink }


### Sending  messages with properties to a JMS provider

For every @scaladoc[JmsMessage](akka.stream.alpakka.jms.JmsMessage$) you can set jms properties.

Scala
: @@snip ($alpakka$/jms/src/test/scala/akka/stream/alpakka/jms/scaladsl/JmsConnectorsSpec.scala) { #create-messages-with-properties }

Java
: @@snip ($alpakka$/jms/src/test/java/akka/stream/alpakka/jms/javadsl/JmsConnectorsTest.java) { #create-messages-with-properties }

### Sending messages with header to a JMS provider
For every @scaladoc[JmsMessage](akka.stream.alpakka.jms.JmsMessage$) you can set also jms headers. 

Scala
: @@snip ($alpakka$/jms/src/test/scala/akka/stream/alpakka/jms/scaladsl/JmsConnectorsSpec.scala) { #create-messages-with-headers }

Java
: @@snip ($alpakka$/jms/src/test/java/akka/stream/alpakka/jms/javadsl/JmsConnectorsTest.java) { #create-messages-with-headers }


### Sending messages as a Flow

The producer can also act as a flow, in order to publish messages in the middle of stream processing.
For example, you can ensure that a message is persisted to the queue before subsequent processing.

Create a flow:

Scala
: @@snip ($alpakka$/jms/src/test/scala/akka/stream/alpakka/jms/scaladsl/JmsConnectorsSpec.scala) { #create-flow-producer }

Java
: @@snip ($alpakka$/jms/src/test/java/akka/stream/alpakka/jms/javadsl/JmsConnectorsTest.java) { #create-flow-producer }

Run the flow:

Scala
: @@snip ($alpakka$/jms/src/test/scala/akka/stream/alpakka/jms/scaladsl/JmsConnectorsSpec.scala) { #run-flow-producer }

Java
: @@snip ($alpakka$/jms/src/test/java/akka/stream/alpakka/jms/javadsl/JmsConnectorsTest.java) { #run-flow-producer }


## Receiving messages from a JMS provider

@java[@scaladoc[JmsConsumer](akka.stream.alpakka.jms.javadsl.JmsConsumer$)]@scala[@scaladoc[JmsConsumer](akka.stream.alpakka.jms.scaladsl.JmsConsumer$)] contains factory methods to facilitate
the creation of sinks according to the message type (see below for an example).


### Receiving @javadoc[String](java.lang.String) messages from a JMS provider

Create a source:

Scala
: @@snip ($alpakka$/jms/src/test/scala/akka/stream/alpakka/jms/scaladsl/JmsConnectorsSpec.scala) { #create-text-source }

Java
: @@snip ($alpakka$/jms/src/test/java/akka/stream/alpakka/jms/javadsl/JmsConnectorsTest.java) { #create-text-source }

The `bufferSize` parameter controls the maximum number of messages to prefetch before applying backpressure.

Run the source and take the same amount of messages as we previously sent to it.

Scala
: @@snip ($alpakka$/jms/src/test/scala/akka/stream/alpakka/jms/scaladsl/JmsConnectorsSpec.scala) { #run-text-source }

Java
: @@snip ($alpakka$/jms/src/test/java/akka/stream/alpakka/jms/javadsl/JmsConnectorsTest.java) { #run-text-source }


### Receiving byte array messages from a JMS provider

Create a source:

Scala
: @@snip ($alpakka$/jms/src/test/scala/akka/stream/alpakka/jms/scaladsl/JmsConnectorsSpec.scala) { #create-bytearray-source }

Java
: @@snip ($alpakka$/jms/src/test/java/akka/stream/alpakka/jms/javadsl/JmsConnectorsTest.java) { #create-bytearray-source }

The `bufferSize` parameter controls the maximum number of messages to prefetch before applying backpressure.

Run the source and take the same amount of messages as we previously sent to it.

Scala
: @@snip ($alpakka$/jms/src/test/scala/akka/stream/alpakka/jms/scaladsl/JmsConnectorsSpec.scala) { #run-bytearray-source }

Java
: @@snip ($alpakka$/jms/src/test/java/akka/stream/alpakka/jms/javadsl/JmsConnectorsTest.java) { #run-bytearray-source }


### Receiving @javadoc[Serializable](java.io.Serializable) object messages from a JMS provider

Create and configure ActiveMQ connection factory to support serialization.
See [ActiveMQ Security](http://activemq.apache.org/objectmessage.html) for more information on this.

Scala
: @@snip ($alpakka$/jms/src/test/scala/akka/stream/alpakka/jms/scaladsl/JmsConnectorsSpec.scala) { #connection-factory-object }

Java
: @@snip ($alpakka$/jms/src/test/java/akka/stream/alpakka/jms/javadsl/JmsConnectorsTest.java) { #connection-factory-object }


Create a source:

Scala
: @@snip ($alpakka$/jms/src/test/scala/akka/stream/alpakka/jms/scaladsl/JmsConnectorsSpec.scala) { #create-object-source }

Java
: @@snip ($alpakka$/jms/src/test/java/akka/stream/alpakka/jms/javadsl/JmsConnectorsTest.java) { #create-object-source }

The `bufferSize` parameter controls the maximum number of messages to prefetch before applying backpressure.

Run the source and take the same amount of messages as we previously sent to it.

Scala
: @@snip ($alpakka$/jms/src/test/scala/akka/stream/alpakka/jms/scaladsl/JmsConnectorsSpec.scala) { #run-object-source }

Java
: @@snip ($alpakka$/jms/src/test/java/akka/stream/alpakka/jms/javadsl/JmsConnectorsTest.java) { #run-object-source }


### Receiving @javadoc[Map](java.util.Map) messages from a JMS provider

Create a source:

Scala
: @@snip ($alpakka$/jms/src/test/scala/akka/stream/alpakka/jms/scaladsl/JmsConnectorsSpec.scala) { #create-map-source }

Java
: @@snip ($alpakka$/jms/src/test/java/akka/stream/alpakka/jms/javadsl/JmsConnectorsTest.java) { #create-map-source }

The `bufferSize` parameter controls the maximum number of messages to prefetch before applying backpressure.

Run the source and take the same amount of messages as we previously sent to it.

Scala
: @@snip ($alpakka$/jms/src/test/scala/akka/stream/alpakka/jms/scaladsl/JmsConnectorsSpec.scala) { #run-map-source }

Java
: @@snip ($alpakka$/jms/src/test/java/akka/stream/alpakka/jms/javadsl/JmsConnectorsTest.java) { #run-map-source }


### Receiving @javadoc[javax.jms.Message](javax.jms.Message)s from a JMS provider

Create a source:

Scala
: @@snip ($alpakka$/jms/src/test/scala/akka/stream/alpakka/jms/scaladsl/JmsConnectorsSpec.scala) { #create-jms-source }

Java
: @@snip ($alpakka$/jms/src/test/java/akka/stream/alpakka/jms/javadsl/JmsConnectorsTest.java) { #create-jms-source }

The `bufferSize` parameter controls the maximum number of messages to prefetch before applying backpressure.

Run the source and specify the amount of messages to take:

Scala
: @@snip ($alpakka$/jms/src/test/scala/akka/stream/alpakka/jms/scaladsl/JmsConnectorsSpec.scala) { #run-jms-source }

Java
: @@snip ($alpakka$/jms/src/test/java/akka/stream/alpakka/jms/javadsl/JmsConnectorsTest.java) { #run-jms-source }

**Notes:**

*  The default `AcknowledgeMode` is `AutoAcknowledge` but can be overridden to custom `AcknowledgeMode`s, even implementation-specific ones by setting the `AcknowledgeMode` in the `JmsConsumerSettings` when creating the stream.

### Receiving @javadoc[javax.jms.Message](javax.jms.Message)s messages from a JMS provider with Client Acknowledgement

Create a @javadoc[javax.jms.Message](javax.jms.Message) source:

Scala
: @@snip ($alpakka$/jms/src/test/scala/akka/stream/alpakka/jms/scaladsl/JmsConnectorsSpec.scala) { #create-jms-source-client-ack }

Java
: @@snip ($alpakka$/jms/src/test/java/akka/stream/alpakka/jms/javadsl/JmsConnectorsTest.java) { #create-jms-source-client-ack }

The `acknowledgeMode` (@scaladoc[AcknowledgeMode](akka.stream.alpakka.jms.AcknowledgeMode$)) parameter controls the JMS acknowledge mode parameter, see @javadoc[javax.jms.Connection.createSession](javax.jms.Connection#createSession-boolean-int-).

Run the source and take the same amount of messages as we previously sent to it acknowledging them.

Scala
: @@snip ($alpakka$/jms/src/test/scala/akka/stream/alpakka/jms/scaladsl/JmsConnectorsSpec.scala) { #run-jms-source-with-ack }

Java
: @@snip ($alpakka$/jms/src/test/java/akka/stream/alpakka/jms/javadsl/JmsConnectorsTest.java) { #run-jms-source-with-ack }


### Receiving @javadoc[javax.jms.Message](javax.jms.Message)s from a JMS provider with a selector

Create a @javadoc[javax.jms.Message](javax.jms.Message) source specifying a [JMS selector expression](https://docs.oracle.com/cd/E19798-01/821-1841/bncer/index.html):

Scala
: @@snip ($alpakka$/jms/src/test/scala/akka/stream/alpakka/jms/scaladsl/JmsConnectorsSpec.scala) {             #create-jms-source-with-selector }

Java
: @@snip ($alpakka$/jms/src/test/java/akka/stream/alpakka/jms/javadsl/JmsConnectorsTest.java) { #create-jms-source-with-selector }

Verify that we are only receiving messages according to the selector:

Scala
: @@snip ($alpakka$/jms/src/test/scala/akka/stream/alpakka/jms/scaladsl/JmsConnectorsSpec.scala) { #assert-only-odd-messages-received }

Java
: @@snip ($alpakka$/jms/src/test/java/akka/stream/alpakka/jms/javadsl/JmsConnectorsTest.java) { #assert-only-odd-messages-received }

### Receiving and explicitly acknowledging @javadoc[javax.jms.Message](javax.jms.Message)s from a JMS provider

Create a @javadoc[javax.jms.Message](javax.jms.Message) source:

Scala
: @@snip ($alpakka$/jms/src/test/scala/akka/stream/alpakka/jms/scaladsl/JmsBufferedAckConnectorsSpec.scala) { #create-jms-source }

Java
: @@snip ($alpakka$/jms/src/test/java/akka/stream/alpakka/jms/javadsl/JmsBufferedAckConnectorsTest.java) { #create-jms-source }

The `sessionCount` parameter controls the number of JMS sessions to run in parallel.

The `bufferSize` parameter controls the maximum number of messages each JMS session will prefetch and awaiting acknowledgement before applying backpressure.

Run the source and specify the amount of messages to take:

Scala
: @@snip ($alpakka$/jms/src/test/scala/akka/stream/alpakka/jms/scaladsl/JmsBufferedAckConnectorsSpec.scala) { #run-jms-source }

Java
: @@snip ($alpakka$/jms/src/test/java/akka/stream/alpakka/jms/javadsl/JmsBufferedAckConnectorsTest.java) { #run-jms-source }

**Notes:**

*  Using multiple sessions increases throughput, especially if a acknowledging message by message is desired.
*  Messages may arrive out of order if `sessionCount` is larger than 1.
*  Message-by-message acknowledgement can be achieved by setting `bufferSize` to 0, thus disabling buffering. The outstanding messages before backpressure will be the `sessionCount`.
*  The default `AcknowledgeMode` is `ClientAcknowledge` but can be overridden to custom `AcknowledgeMode`s, even implementation-specific ones by setting the `AcknowledgeMode` in the `JmsConsumerSettings` when creating the stream. 

### Transactionally receiving @javadoc[javax.jms.Message](javax.jms.Message)s from a JMS provider

Create a @javadoc[javax.jms.Message](javax.jms.Message) source:

Scala
: @@snip ($alpakka$/jms/src/test/scala/akka/stream/alpakka/jms/scaladsl/JmsTxConnectorsSpec.scala) { #create-jms-source }

Java
: @@snip ($alpakka$/jms/src/test/java/akka/stream/alpakka/jms/javadsl/JmsTxConnectorsTest.java) { #create-jms-source }

The `sessionCount` parameter controls the number of JMS sessions to run in parallel.

The `bufferSize` parameter controls the maximum number of messages each JMS session will prefetch and awaiting acknowledgement before applying backpressure.

Run the source and specify the amount of messages to take:

Scala
: @@snip ($alpakka$/jms/src/test/scala/akka/stream/alpakka/jms/scaladsl/JmsTxConnectorsSpec.scala) { #run-jms-source }

Java
: @@snip ($alpakka$/jms/src/test/java/akka/stream/alpakka/jms/javadsl/JmsTxConnectorsTest.java) { #run-jms-source }

**Notes:**

*  Higher throughput is achieved by increasing the `sessionCount`.
*  Messages will arrive out of order if `sessionCount` is larger than 1.
*  Buffering is not supported in transaction mode. The `bufferSize` is ignored.
*  The default `AcknowledgeMode` is `SessionTransacted` but can be overridden to custom `AcknowledgeMode`s, even implementation-specific ones by setting the `AcknowledgeMode` in the `JmsConsumerSettings` when creating the stream. 
 
### Browsing messages from a JMS provider

The browse source will stream the messages in a queue without consuming them.

Create a source:

Scala
: @@snip ($alpakka$/jms/src/test/scala/akka/stream/alpakka/jms/scaladsl/JmsConnectorsSpec.scala) { #create-browse-source }

Java
: @@snip ($alpakka$/jms/src/test/java/akka/stream/alpakka/jms/javadsl/JmsConnectorsTest.java) { #create-browse-source }

The `messageSelector` parameter can be used to filter the messages. Otherwise it will browse the entire content of the queue.

Unlike the other sources, the browse source will complete after browsing all the messages:

Scala
: @@snip ($alpakka$/jms/src/test/scala/akka/stream/alpakka/jms/scaladsl/JmsConnectorsSpec.scala) { #run-browse-source }

Java
: @@snip ($alpakka$/jms/src/test/java/akka/stream/alpakka/jms/javadsl/JmsConnectorsTest.java) { #run-browse-source }

**Notes:**

*  Messages may be arriving and expiring while the scan is done.
*  The JMS API does not require the content of an enumeration to be a static snapshot of queue content. Whether these changes are visible or not depends on the JMS provider.
*  A message must not be returned by a QueueBrowser before its delivery time has been reached.


### Using Topic with an JMS provider

You can use JMS topic in a very similar way.

For the Sink :

Scala
: @@snip ($alpakka$/jms/src/test/scala/akka/stream/alpakka/jms/scaladsl/JmsConnectorsSpec.scala) { #create-topic-sink }

Java
: @@snip ($alpakka$/jms/src/test/java/akka/stream/alpakka/jms/javadsl/JmsConnectorsTest.java) { #create-topic-sink }

For the source :

Scala
: @@snip ($alpakka$/jms/src/test/scala/akka/stream/alpakka/jms/scaladsl/JmsConnectorsSpec.scala) { #create-topic-source }

Java
: @@snip ($alpakka$/jms/src/test/java/akka/stream/alpakka/jms/javadsl/JmsConnectorsTest.java) { #create-topic-source }

Such sink and source can be started the same way as in the previous example.

**Notes:**

* Explicit acknowledgement sources and transactional sources work with topics the same way they work with queues.
* **DO NOT** set the `sessionCount` greater than 1 for topics. Doing so will result in duplicate messages being delivered. Each topic message is delivered to each JMS session and all the messages feed to the same `Source`. JMS 2.0 created shared consumers to solve this problem and multiple sessions without duplication may be supported in the future.

### Running the example code

The code in this guide is part of runnable tests of this project. You are welcome to edit the code and run it in sbt.

Scala
:   ```
    sbt
    > jms/testOnly *.JmsConnectorsSpec
    ```

Java
:   ```
    sbt
    > jms/testOnly *.JmsConnectorsTest
    ```
    
### Stopping a JMS Source

All JMS sources materialize to a `KillSwitch` to allow safely stopping consumption without message loss for transactional and acknowledged messages, and with minimal message loss for the simple JMS source.

To stop consumption safely, call `shutdown()` on the `KillSwitch` that is the materialized value of the source. To abruptly abort consumption (without concerns for message loss), call `abort(Throwable)` on the `KillSwitch`.

## Using IBM MQ

You can use IBM MQ like any other JMS Provider by creating a `QueueConnectionFactory` or a `TopicConnectionFactory`
and creating a `JmsConsumerSettings` or `JmsProducerSettings` from it.
The below snippets have been tested with a default IBM MQ docker image which contains queues and topics for testing.
The following command starts MQ 9 using docker:

    docker run --env LICENSE=accept --env MQ_QMGR_NAME=QM1 --publish 1414:1414 --publish 9443:9443 ibmcom/mq:9

MQ settings for this image are shown here: https://github.com/ibm-messaging/mq-docker#mq-developer-defaults

### Create a JmsConsumer to an IBM MQ Queue

The `MQQueueConnectionFactory` needs a queue manager name and a channel name, the docker command used in the previous section sets up a `QM1` queue manager and a `DEV.APP.SVRCONN` channel. The IBM MQ client makes it possible to
connect to the MQ server over TCP/IP or natively through JNI (when the client and server run on the same machine). In the examples below we have chosen to use TCP/IP, which is done by setting the transport type to `CommonConstants.WMQ_CM_CLIENT`.

Scala
:   ```
    import com.ibm.mq.jms.MQQueueConnectionFactory
    import com.ibm.msg.client.wmq.common.CommonConstants
    val QueueManagerName = "QM1"
    val TestChannelName = "DEV.APP.SVRCONN"
    // Create the IBM MQ QueueConnectionFactory
    val queueConnectionFactory = new MQQueueConnectionFactory()
    queueConnectionFactory.setQueueManager(QueueManagerName)
    queueConnectionFactory.setChannel(TestChannelName)
    // Connect to IBM MQ over TCP/IP
    queueConnectionFactory.setTransportType(CommonConstants.WMQ_CM_CLIENT)
    val TestQueueName = "DEV.QUEUE.1"
    val jmsSource: Source[String, NotUsed] = JmsConsumer.textSource(
      JmsConsumerSettings(queueConnectionFactory).withQueue(TestQueueName)
    )
    ```

Java
:   ```
    import com.ibm.mq.jms.MQQueueConnectionFactory;
    import com.ibm.msg.client.wmq.common.CommonConstants;
    String queueManagerName = "QM1";
    String testChannelName = "DEV.APP.SVRCONN";
    // Create the IBM MQ QueueConnectionFactory
    MQQueueConnectionFactory queueConnectionFactory = new MQQueueConnectionFactory();
    queueConnectionFactory.setQueueManager(queueManagerName);
    queueConnectionFactory.setChannel(testChannelName);
    // Connect to IBM MQ over TCP/IP
    queueConnectionFactory.setTransportType(CommonConstants.WMQ_CM_CLIENT);
    String testQueueName = "DEV.QUEUE.1";
    Source<String, NotUsed> jmsSource = JmsConsumer.textSource(
      JmsConsumerSettings
        .create(queueConnectionFactory)
        .withQueue(testQueueName)
    );
    ```

### Create a JmsProducer to an IBM MQ Topic
The IBM MQ docker container sets up a `dev/` topic, which is used in the example below.

Scala
:   ```
    import com.ibm.mq.jms.MQTopicConnectionFactory
    import com.ibm.msg.client.wmq.common.CommonConstants
    val QueueManagerName = "QM1"
    val TestChannelName = "DEV.APP.SVRCONN"
    // Create the IBM MQ TopicConnectionFactory
    val topicConnectionFactory = new MQTopicConnectionFactory()
    topicConnectionFactory.setQueueManager(QueueManagerName)
    topicConnectionFactory.setChannel(TestChannelName)
    // Connect to IBM MQ over TCP/IP
    topicConnectionFactory.setTransportType(CommonConstants.WMQ_CM_CLIENT)
    val TestTopicName = "dev/"
    val jmsTopicSink: Sink[String, NotUsed] = JmsProducer(
      JmsProducerSettings(topicConnectionFactory).withTopic(TestTopicName)
    )
    ```

Java
:   ```
    import com.ibm.mq.jms.MQTopicConnectionFactory;
    import com.ibm.msg.client.wmq.common.CommonConstants;
    String queueManagerName = "QM1";
    String testChannelName = "DEV.APP.SVRCONN";
    // Create the IBM MQ TopicConnectionFactory
    val topicConnectionFactory = new MQTopicConnectionFactory();
    topicConnectionFactory.setQueueManager(queueManagerName);
    topicConnectionFactory.setChannel(testChannelName);
    // Connect to IBM MQ over TCP/IP
    topicConnectionFactory.setTransportType(CommonConstants.WMQ_CM_CLIENT);
    String testTopicName = "dev/";
    Sink<String, NotUsed> jmsTopicSink = JmsProducer.textSink(
      JmsProducerSettings
        .create(topicConnectionFactory)
        .withTopic(testTopicName)
    );
    ```