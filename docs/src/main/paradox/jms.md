# JMS Connector

The JMS connector provides Akka Stream sources and sinks to connect to JMS providers.

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

The JMS message model supports several types of message body (see @extref(javaee-api:javax.jms.Message)) and Alpakka currently supports messages with a body containing a @extref[String](java-api:java.lang.String), 
a @extref[Serializable](java-api:java.io.Serializable) object, a @extref[Map](java-api:java.util.Map) or a byte array.

Use a case class with the subtype of @scaladoc[JmsMessage](akka.stream.alpakka.jms.JmsMessage$) to wrap the messages you want to send and optionally set their properties (see below for an example).

### Sending messages to a JMS provider

First define a jms `ConnectionFactory` depending on the implementation you're using. Here we're using Active MQ.

Scala
: @@snip (../../../../jms/src/test/scala/akka/stream/alpakka/jms/scaladsl/JmsConnectorsSpec.scala) { #connection-factory }

Java
: @@snip (../../../../jms/src/test/java/akka/stream/alpakka/jms/javadsl/JmsConnectorsTest.java) { #connection-factory }


Create a sink, that accepts and forwards @extref[String](java-api:java.lang.String)s to the JMS provider.

Scala
: @@snip (../../../../jms/src/test/scala/akka/stream/alpakka/jms/scaladsl/JmsConnectorsSpec.scala) { #create-text-sink }

Java
: @@snip (../../../../jms/src/test/java/akka/stream/alpakka/jms/javadsl/JmsConnectorsTest.java) { #create-text-sink }

@scaladoc[JmsSink](akka.stream.alpakka.jms.JmsSink$) contains factory methods to facilitate the creation of sinks.

Last step is to @extref[materialize](akka-docs:scala/stream/stream-flows-and-basics) and run the sink(s) we have created.

Scala
: @@snip (../../../../jms/src/test/scala/akka/stream/alpakka/jms/scaladsl/JmsConnectorsSpec.scala) { #run-text-sink }

Java
: @@snip (../../../../jms/src/test/java/akka/stream/alpakka/jms/javadsl/JmsConnectorsTest.java) { #run-text-sink }

### Sending text messages with properties to a JMS provider

Create a sink, that accepts and forwards @scaladoc[JmsTextMessage](akka.stream.alpakka.jms.JmsTextMessage$)s to the JMS provider:

Scala
: @@snip (../../../../jms/src/test/scala/akka/stream/alpakka/jms/scaladsl/JmsConnectorsSpec.scala) { #create-messages-with-properties }

Java
: @@snip (../../../../jms/src/test/java/akka/stream/alpakka/jms/javadsl/JmsConnectorsTest.java) { #create-test-message-list #create-messages-with-properties }

#### Sending byte messages to a JMS provider

Create a sink, that accepts and forwards @scaladoc[JmsByteMessage](akka.stream.alpakka.jms.JmsByteMessage$)s to the JMS provider. 

Scala
: @@snip (../../../../jms/src/test/scala/akka/stream/alpakka/jms/scaladsl/JmsConnectorsSpec.scala) { #create-bytearray-sink }

Java
: @@snip (../../../../jms/src/test/java/akka/stream/alpakka/jms/javadsl/JmsConnectorsTest.java) { #create-bytearray-sink  }

#### Sending map messages to a JMS provider

Create a sink, that accepts and forwards @scaladoc[JmsMapMessage](akka.stream.alpakka.jms.JmsMapMessage$)s to the JMS provider:

Scala
: @@snip (../../../../jms/src/test/scala/akka/stream/alpakka/jms/scaladsl/JmsConnectorsSpec.scala) { #create-map-sink }

Java
: @@snip (../../../../jms/src/test/java/akka/stream/alpakka/jms/javadsl/JmsConnectorsTest.java) { #create-map-sink }

#### Sending object messages to a JMS provider

Create a sink, that accepts and forwards @scaladoc[JmsObjectMessage](akka.stream.alpakka.jms.JmsObjectMessage$)s to the JMS provider:

Scala
: @@snip (../../../../jms/src/test/scala/akka/stream/alpakka/jms/scaladsl/JmsConnectorsSpec.scala) { #create-object-sink }

Java
: @@snip (../../../../jms/src/test/java/akka/stream/alpakka/jms/javadsl/JmsConnectorsTest.java) { #create-object-sink }

### Receiving @extref[String](java-api:java.lang.String) messages from a JMS provider

Create a source:

Scala
: @@snip (../../../../jms/src/test/scala/akka/stream/alpakka/jms/scaladsl/JmsConnectorsSpec.scala) { #create-text-source }

Java
: @@snip (../../../../jms/src/test/java/akka/stream/alpakka/jms/javadsl/JmsConnectorsTest.java) { #create-text-source }

The `bufferSize` parameter controls the maximum number of messages to prefetch before applying backpressure.

Run the source and take the same amount of messages as we previously sent to it.

Scala
: @@snip (../../../../jms/src/test/scala/akka/stream/alpakka/jms/scaladsl/JmsConnectorsSpec.scala) { #run-text-source }

Java
: @@snip (../../../../jms/src/test/java/akka/stream/alpakka/jms/javadsl/JmsConnectorsTest.java) { #run-text-source }


### Receiving byte array messages from a JMS provider

Create a source:

Scala
: @@snip (../../../../jms/src/test/scala/akka/stream/alpakka/jms/scaladsl/JmsConnectorsSpec.scala) { #create-bytearray-source }

Java
: @@snip (../../../../jms/src/test/java/akka/stream/alpakka/jms/javadsl/JmsConnectorsTest.java) { #create-bytearray-source }

The `bufferSize` parameter controls the maximum number of messages to prefetch before applying backpressure.

Run the source and take the same amount of messages as we previously sent to it.

Scala
: @@snip (../../../../jms/src/test/scala/akka/stream/alpakka/jms/scaladsl/JmsConnectorsSpec.scala) { #run-bytearray-source }

Java
: @@snip (../../../../jms/src/test/java/akka/stream/alpakka/jms/javadsl/JmsConnectorsTest.java) { #run-bytearray-source }


### Receiving @extref[Serializable](java-api:java.io.Serializable) object messages from a JMS provider

Create a source:

Scala
: @@snip (../../../../jms/src/test/scala/akka/stream/alpakka/jms/scaladsl/JmsConnectorsSpec.scala) { #create-object-source }

Java
: @@snip (../../../../jms/src/test/java/akka/stream/alpakka/jms/javadsl/JmsConnectorsTest.java) { #create-object-source }

The `bufferSize` parameter controls the maximum number of messages to prefetch before applying backpressure.

Run the source and take the same amount of messages as we previously sent to it.

Scala
: @@snip (../../../../jms/src/test/scala/akka/stream/alpakka/jms/scaladsl/JmsConnectorsSpec.scala) { #run-object-source }

Java
: @@snip (../../../../jms/src/test/java/akka/stream/alpakka/jms/javadsl/JmsConnectorsTest.java) { #run-object-source }


### Receiving @extref[Map](java-api:java.util.Map) messages from a JMS provider

Create a source:

Scala
: @@snip (../../../../jms/src/test/scala/akka/stream/alpakka/jms/scaladsl/JmsConnectorsSpec.scala) { #create-map-source }

Java
: @@snip (../../../../jms/src/test/java/akka/stream/alpakka/jms/javadsl/JmsConnectorsTest.java) { #create-map-source }

The `bufferSize` parameter controls the maximum number of messages to prefetch before applying backpressure.

Run the source and take the same amount of messages as we previously sent to it.

Scala
: @@snip (../../../../jms/src/test/scala/akka/stream/alpakka/jms/scaladsl/JmsConnectorsSpec.scala) { #run-map-source }

Java
: @@snip (../../../../jms/src/test/java/akka/stream/alpakka/jms/javadsl/JmsConnectorsTest.java) { #run-map-source }


### Receiving @extref[javax.jms.Message](javaee-api:javax.jms.Message)s from a JMS provider

Create a @extref[javax.jms.Message](javaee-api:javax.jms.Message) source:

Scala
: @@snip (../../../../jms/src/test/scala/akka/stream/alpakka/jms/scaladsl/JmsConnectorsSpec.scala) { #create-jms-source }

Java
: @@snip (../../../../jms/src/test/java/akka/stream/alpakka/jms/javadsl/JmsConnectorsTest.java) { #create-jms-source }

The `bufferSize` parameter controls the maximum number of messages to prefetch before applying backpressure.

Run the source and specify the amount of messages to take:

Scala
: @@snip (../../../../jms/src/test/scala/akka/stream/alpakka/jms/scaladsl/JmsConnectorsSpec.scala) { #run-jms-source }

Java
: @@snip (../../../../jms/src/test/java/akka/stream/alpakka/jms/javadsl/JmsConnectorsTest.java) { #run-jms-source }

### Receiving @extref[javax.jms.Message](javaee-api:javax.jms.Message)s from a JMS provider with a selector

Create a @extref[javax.jms.Message](javaee-api:javax.jms.Message) source specifying a [JMS selector expression](https://docs.oracle.com/cd/E19798-01/821-1841/bncer/index.html):

Scala
: @@snip (../../../../jms/src/test/scala/akka/stream/alpakka/jms/scaladsl/JmsConnectorsSpec.scala) {             #create-jms-source-with-selector }

Java
: @@snip (../../../../jms/src/test/java/akka/stream/alpakka/jms/javadsl/JmsConnectorsTest.java) { #create-jms-source-with-selector }

Verify that we are only receiving messages according to the selector:

Scala
: @@snip (../../../../jms/src/test/scala/akka/stream/alpakka/jms/scaladsl/JmsConnectorsSpec.scala) { #assert-only-odd-messages-received }

Java
: @@snip (../../../../jms/src/test/java/akka/stream/alpakka/jms/javadsl/JmsConnectorsTest.java) { #assert-only-odd-messages-received }

### Using Topic with an JMS provider

You can use JMS topic in a very similar way.

For the Sink :

Scala
: @@snip (../../../../jms/src/test/scala/akka/stream/alpakka/jms/scaladsl/JmsConnectorsSpec.scala) { #create-topic-sink }

Java
: @@snip (../../../../jms/src/test/java/akka/stream/alpakka/jms/javadsl/JmsConnectorsTest.java) { #create-topic-sink }

For the source :

Scala
: @@snip (../../../../jms/src/test/scala/akka/stream/alpakka/jms/scaladsl/JmsConnectorsSpec.scala) { #create-topic-source }

Java
: @@snip (../../../../jms/src/test/java/akka/stream/alpakka/jms/javadsl/JmsConnectorsTest.java) { #create-topic-source }

Such sink and source can be started the same way as in the previous example.

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
