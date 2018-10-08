# MQTT Streaming

@@@ note { title="MQTT Streaming" }

MQTT stands for MQ Telemetry Transport. It is a publish/subscribe, extremely simple and lightweight messaging protocol, designed for constrained devices and low-bandwidth, high-latency or unreliable networks. The design principles are to minimise network bandwidth and device resource requirements whilst also attempting to ensure reliability and some degree of assurance of delivery. These principles also turn out to make the protocol ideal of the emerging “machine-to-machine” (M2M) or “Internet of Things” world of connected devices, and for mobile applications where bandwidth and battery power are at a premium.  

Further information on [mqtt.org](https://mqtt.org/).

@@@ 

The Alpakka MQTT connector provides an Akka Stream flow to connect to MQTT brokers. In addition, a flow is provided so that you can implement your own MQTT server in the case where you do not wish to use a broker. MQTT is a fine protocol for directed client/server interactions, as well as having an intermediary broker.

The library has no dependencies other than those of Akka Streams i.e. it is entirely reactive. As such, there should be a significant performance advantage given its pure-Akka foundations.

### Reported issues

[Tagged issues at Github](https://github.com/akka/alpakka/labels/p%3Amqtt-streaming)

## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-mqtt-streaming_$scala.binary.version$
  version=$project.version$
}

## Flow through a client session

The following code illustrates how to establish an MQTT client session and join it with a TCP connection:

Scala
: @@snip [snip](/mqtt-streaming/src/test/scala/docs/scaladsl/MqttFlowSpec.scala) { #create-streaming-flow }

Java
: @@snip [snip](/mqtt-streaming/src/test/java/docs/javadsl/MqttFlowTest.java) { #create-streaming-flow }

The resulting flow's type shows how `Command`s are received and `Event`s are emitted. With `Event`, they can
be either decoded successfully or not.

Run the flow by connecting a source of messages to be published via a queue:

Scala
: @@snip [snip](/mqtt-streaming/src/test/scala/docs/scaladsl/MqttFlowSpec.scala) { #run-streaming-flow }

Java
: @@snip [snip](/mqtt-streaming/src/test/java/docs/javadsl/MqttFlowTest.java) { #run-streaming-flow }

We drop the first 3 events received as they will be ACKs to our connect, subscribe and publish. The next event
received is the publication to the topic we also subscribed to.

## Running the example code

The code in this guide is part of runnable tests of this project. You are welcome to edit the code and run it in sbt.

Scala
:   ```
    sbt
    > mqtt-streaming/testOnly *.MqttFlowSpec
    ```

Java
:   ```
    sbt
    > mqtt-streaming/testOnly *.MqttFlowTest
    ```
