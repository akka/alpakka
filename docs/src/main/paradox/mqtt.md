# MQTT Connector

The MQTT connector provides Akka Stream source, sink and flow to connect to MQTT servers.

## Artifacts

sbt
:   @@@vars
    ```scala
    libraryDependencies += "com.lightbend.akka" %% "akka-stream-alpakka-mqtt" % "$version$"
    ```
    @@@

Maven
:   @@@vars
    ```xml
    <dependency>
      <groupId>com.lightbend.akka</groupId>
      <artifactId>akka-stream-alpakka-mqtt_$scalaBinaryVersion$</artifactId>
      <version>$version$</version>
    </dependency>
    ```
    @@@

Gradle
:   @@@vars
    ```gradle
    dependencies {
      compile group: "com.lightbend.akka", name: "akka-stream-alpakka-mqtt_$scalaBinaryVersion$", version: "$version$"
    }
    ```
    @@@

## Usage

First we need to define various settings, that are required when connecting to an MQTT server.

Scala
: @@snip (../../../../mqtt/src/test/scala/akka/stream/alpakka/mqtt/scaladsl/MqttSourceSpec.scala) { #create-connection-settings }

Java
: @@snip (../../../../mqtt/src/test/java/akka/stream/alpakka/mqtt/javadsl/MqttSourceTest.java) { #create-connection-settings }

Here we used @scaladoc[MqttConnectionSettings](akka.stream.alpakka.mqtt.MqttConnectionSettings$) factory to set the address of the server, client ID, which needs to be unique for every client, and client persistence implementation (@extref[MemoryPersistence](paho-api:org/eclipse/paho/client/mqttv3/persist/MemoryPersistence)) which allows to control reliability guarantees.

Then let's create a source that is going to connect to the MQTT server upon materialization and receive messages that are sent to the subscribed topics.

Scala
: @@snip (../../../../mqtt/src/test/scala/akka/stream/alpakka/mqtt/scaladsl/MqttSourceSpec.scala) { #create-source }

Java
: @@snip (../../../../mqtt/src/test/java/akka/stream/alpakka/mqtt/javadsl/MqttSourceTest.java) { #create-source }

And finally run the source.

Scala
: @@snip (../../../../mqtt/src/test/scala/akka/stream/alpakka/mqtt/scaladsl/MqttSourceSpec.scala) { #run-source }

Java
: @@snip (../../../../mqtt/src/test/java/akka/stream/alpakka/mqtt/javadsl/MqttSourceTest.java) { #run-source }

This source has a materialized value (@scaladoc[Future](scala.concurrent.Future) in Scala API and @extref[CompletionStage](java-api:java/util/concurrent/CompletionStage) in Java API) which is completed when the subscription to the MQTT broker has been completed.

To publish messages to the MQTT server create a sink and run it.

Scala
: @@snip (../../../../mqtt/src/test/scala/akka/stream/alpakka/mqtt/scaladsl/MqttSourceSpec.scala) { #run-sink }

Java
: @@snip (../../../../mqtt/src/test/java/akka/stream/alpakka/mqtt/javadsl/MqttSourceTest.java) { #run-sink }

To connect to the MQTT server in a bidirectional way (source and sink combined) and with the same client ID (using a single instance of the underlaying client) create a flow and connect its inlet and outlet accordingly.

Scala
: @@snip (../../../../mqtt/src/test/scala/akka/stream/alpakka/mqtt/scaladsl/MqttFlowSpec.scala) { #run-flow }

Java
: @@snip (../../../../mqtt/src/test/java/akka/stream/alpakka/mqtt/javadsl/MqttFlowTest.java) { #run-flow }

### Running the example code

The code in this guide is part of runnable tests of this project. You are welcome to edit the code and run it in sbt.

Scala
:   ```
    sbt
    > mqtt/testOnly *.MqttSourceSpec
    ```

Java
:   ```
    sbt
    > mqtt/testOnly *.MqttSourceTest
    ```
