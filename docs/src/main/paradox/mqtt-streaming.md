# MQTT Streaming

@@@ note { title="MQTT Streaming" }

MQTT stands for MQ Telemetry Transport. It is a publish/subscribe, extremely simple and lightweight messaging protocol, designed for constrained devices and low-bandwidth, high-latency or unreliable networks. The design principles are to minimise network bandwidth and device resource requirements whilst also attempting to ensure reliability and some degree of assurance of delivery. These principles also turn out to make the protocol ideal of the emerging “machine-to-machine” (M2M) or “Internet of Things” world of connected devices, and for mobile applications where bandwidth and battery power are at a premium.  

Further information on [mqtt.org](https://mqtt.org/).

@@@ 

The Alpakka MQTT connector provides an Akka Stream source, sink and flow to connect to MQTT brokers. It has no dependencies other than those of Akka Streams i.e. it is entirely reactive. As such, there should be a significant performance advantage given its pure-Akka foundations.

### Reported issues

[Tagged issues at Github](https://github.com/akka/alpakka/labels/p%3Amqtt-streaming)

## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-mqtt-streaming_$scala.binary.version$
  version=$project.version$
}

## Reading from MQTT

### At most once

Then let's create a source that connects to the MQTT server and receives messages from the subscribed topics.

Scala
: @@snip [snip](/mqtt/src/test/scala/docs/scaladsl/MqttSourceSpec.scala) { #create-source }

Java
: @@snip [snip](/mqtt/src/test/java/docs/javadsl/MqttSourceTest.java) { #create-source }

This source has a materialized value (@scala[@scaladoc[Future[Done]](scala.concurrent.Future)]@java[@javadoc[CompletionStage&lt;Done&gt;](java.util.concurrent.CompletionStage)]) which is completed when the subscription to the MQTT broker has been established.

MQTT `atMostOnce` automatically acknowledges messages back to the server when they are passed downstream. 

### At least once

The `atLeastOnce` source allow users to acknowledge the messages anywhere downstream.
Please note that for manual acks to work `CleanSession` should be set to false and `MqttQoS` should be `AtLeastOnce`.

Scala
: @@snip [snip](/mqtt/src/test/scala/docs/scaladsl/MqttSourceSpec.scala) { #create-source-with-manualacks }

Java
: @@snip [snip](/mqtt/src/test/java/docs/javadsl/MqttSourceTest.java) { #create-source-with-manualacks }


The `atLeastOnce` source returns @scala[@scaladoc[MqttMessageWithAck](akka.stream.alpakka.mqtt.streaming.scaladsl.MqttMessageWithAck)]@java[@scaladoc[MqttMessageWithAck](akka.stream.alpakka.mqtt.streaming.javadsl.MqttMessageWithAck)] so you can acknowledge them by calling `ack()`.

Scala
: @@snip [snip](/mqtt/src/test/scala/docs/scaladsl/MqttSourceSpec.scala) { #run-source-with-manualacks }

Java
: @@snip [snip](/mqtt/src/test/java/docs/javadsl/MqttSourceTest.java) { #run-source-with-manualacks }


## Publishing to MQTT

To publish messages to the MQTT server create a sink by specifying `MqttConnectionSettings` ([API](akka.stream.alpakka.mqtt.streaming.MqttConnectionSettings$)) and a default Quality of Service-level.

Scala
: @@snip [snip](/mqtt/src/test/scala/docs/scaladsl/MqttSourceSpec.scala) { #run-sink }

Java
: @@snip [snip](/mqtt/src/test/java/docs/javadsl/MqttSourceTest.java) { #run-sink }


The Quality of Service-level and the retained flag can be configured on a per-message basis.

Scala
: @@snip [snip](/mqtt/src/test/scala/docs/scaladsl/MqttSourceSpec.scala) { #will-message }

Java
: @@snip [snip](/mqtt/src/test/java/docs/javadsl/MqttSourceTest.java) { #will-message }


## Publish and subscribe in a single flow

It is also possible to connect to the MQTT server in bidirectional fashion, using a single underlying connection (and client ID). To do that create an MQTT flow that combines the functionalities of an MQTT source and an MQTT sink.

Scala
: @@snip [snip](/mqtt/src/test/scala/docs/scaladsl/MqttFlowSpec.scala) { #create-flow }

Java
: @@snip [snip](/mqtt/src/test/java/docs/javadsl/MqttFlowTest.java) { #create-flow }


Run the flow by connecting a source of messages to be published and a sink for received messages.

Scala
: @@snip [snip](/mqtt/src/test/scala/docs/scaladsl/MqttFlowSpec.scala) { #run-flow }

Java
: @@snip [snip](/mqtt/src/test/java/docs/javadsl/MqttFlowTest.java) { #run-flow }


## Running the example code

The code in this guide is part of runnable tests of this project. You are welcome to edit the code and run it in sbt.

Scala
:   ```
    sbt
    > mqtt-streaming/testOnly *.MqttSourceSpec
    ```

Java
:   ```
    sbt
    > mqtt-streaming/testOnly *.MqttSourceTest
    ```
