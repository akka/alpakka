# JMS Connector

The JMS connector provides Akka Stream sources and sinks to connect to JMS servers.

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

### Sending messages to JMS server

First define a jms `ConnectionFactory` depending on the implementation you're using. Here we're using Active MQ.

Scala
: @@snip (../../../../jms/src/test/scala/akka/stream/alpakka/jms/scaladsl/JmsConnectorsSpec.scala) { #connection-factory }

Java
: @@snip (../../../../jms/src/test/java/akka/stream/alpakka/jms/javadsl/JmsConnectorsTest.java) { #connection-factory }


Create a sink, that accepts and forwards @extref[String](java-api:java.lang.String)s to the JMS server.

Scala
: @@snip (../../../../jms/src/test/scala/akka/stream/alpakka/jms/scaladsl/JmsConnectorsSpec.scala) { #create-sink }

Java
: @@snip (../../../../jms/src/test/java/akka/stream/alpakka/jms/javadsl/JmsConnectorsTest.java) { #create-sink }

@scaladoc[JmsSink](akka.stream.alpakka.jms.JmsSink$) is a collection of factory methods that facilitates creation of sinks.

Last step is to @extref[materialize](akka-docs:scala/stream/stream-flows-and-basics) and run the sink we have created.

Scala
: @@snip (../../../../jms/src/test/scala/akka/stream/alpakka/jms/scaladsl/JmsConnectorsSpec.scala) { #run-sink }

Java
: @@snip (../../../../jms/src/test/java/akka/stream/alpakka/jms/javadsl/JmsConnectorsTest.java) { #run-sink }

### Receiving messages from JMS server

Create a source using the same queue declaration as before.

Scala
: @@snip (../../../../jms/src/test/scala/akka/stream/alpakka/jms/scaladsl/JmsConnectorsSpec.scala) { #create-source }

Java
: @@snip (../../../../jms/src/test/java/akka/stream/alpakka/jms/javadsl/JmsConnectorsTest.java) { #create-source }

The `bufferSize` parameter controls the maximum number of messages to prefetch before applying backpressure.

Run the source and take the same amount of messages as we previously sent to it.

Scala
: @@snip (../../../../jms/src/test/scala/akka/stream/alpakka/jms/scaladsl/JmsConnectorsSpec.scala) { #run-source }

Java
: @@snip (../../../../jms/src/test/java/akka/stream/alpakka/jms/javadsl/JmsConnectorsTest.java) { #run-source }

This is how you send and receive message from JMS server using this connector.

### Using Topic with an JMS server

You can use JMS topic in q very similar way.

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
