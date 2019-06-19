# MQTT

@@@ note { title="MQTT" }

MQTT stands for MQ Telemetry Transport. It is a publish/subscribe, extremely simple and lightweight messaging protocol, designed for constrained devices and low-bandwidth, high-latency or unreliable networks. The design principles are to minimise network bandwidth and device resource requirements whilst also attempting to ensure reliability and some degree of assurance of delivery. These principles also turn out to make the protocol ideal of the emerging “machine-to-machine” (M2M) or “Internet of Things” world of connected devices, and for mobile applications where bandwidth and battery power are at a premium.  

Further information on [mqtt.org](https://mqtt.org/).

@@@ 

@@@ note { title="Streaming Differences" }

Alpakka contains @ref[another MQTT connector](mqtt-streaming.md) which is _not_ based on the Eclipse Paho client, unlike this one. Please refer to the other connector where the differences are expanded on.

@@@

The Alpakka MQTT connector provides an Akka Stream source, sink and flow to connect to MQTT brokers. It is based on the [Eclipse Paho Java client](https://www.eclipse.org/paho/clients/java/).

@@project-info{ projectId="mqtt" }

## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-mqtt_$scala.binary.version$
  version=$project.version$
}

The table below shows direct dependencies of this module and the second tab shows all libraries it depends on transitively.

@@dependencies { projectId="mqtt" }


## Settings

The required `MqttConnectionSettings` (@scaladoc[API](akka.stream.alpakka.mqtt.MqttConnectionSettings$)) settings to connect to an MQTT server are 

1. the MQTT broker address
1. a unique ID for the client (setting it to the empty string should let the MQTT broker assign it, but not all do; you might want to generate it)
1. the MQTT client persistence to use (eg. @javadoc[MemoryPersistence](org.eclipse.paho.client.mqttv3.persist.MemoryPersistence)) which allows to control reliability guarantees 

Scala
: @@snip [snip](/mqtt/src/test/scala/docs/scaladsl/MqttSourceSpec.scala) { #create-connection-settings }

Java
: @@snip [snip](/mqtt/src/test/java/docs/javadsl/MqttSourceTest.java) { #create-connection-settings }

Most settings are passed on to Paho's `MqttConnectOptions` (@javadoc[API](org.eclipse.paho.client.mqttv3.MqttConnectOptions)) and documented there. 

@@@ warning { title='Use delayed stream restarts' }
Note that the following examples do not provide any connection management and are designed to get you going quickly. Consider empty client IDs to auto-generate unique identifiers and the use of [delayed stream restarts](https://doc.akka.io/docs/akka/current/stream/stream-error.html?language=scala#delayed-restarts-with-a-backoff-stage). The underlying Paho library's auto-reconnect feature [does not handle initial connections by design](https://github.com/eclipse/paho.mqtt.golang/issues/77).
@@@


### Configure encrypted connections

To connect with transport-level security configure the address as `ssl://`, set authentication details and pass in a socket factory.

Scala
: @@snip [snip](/mqtt/src/test/scala/docs/scaladsl/MqttSourceSpec.scala) { #ssl-settings }

Java
: @@snip [snip](/mqtt/src/test/java/docs/javadsl/MqttSourceTest.java) { #ssl-settings }


## Reading from MQTT

### At most once

Then let's create a source that connects to the MQTT server and receives messages from the subscribed topics.

The `bufferSize` sets the maximum number of messages read from MQTT before back-pressure applies.


Scala
: @@snip [snip](/mqtt/src/test/scala/docs/scaladsl/MqttSourceSpec.scala) { #create-source }

Java
: @@snip [snip](/mqtt/src/test/java/docs/javadsl/MqttSourceTest.java) { #create-source }

This source has a materialized value (@scala[@scaladoc[Future[Done]](scala.concurrent.Future)]@java[@javadoc[CompletionStage&lt;Done&gt;](java.util.concurrent.CompletionStage)]) which is completed when the subscription to the MQTT broker has been established.

MQTT `atMostOnce` automatically acknowledges messages back to the server when they are passed downstream. 

### At least once

The `atLeastOnce` source allow users to acknowledge the messages anywhere downstream.
Please note that for manual acks to work `CleanSession` should be set to false and `MqttQoS` should be `AtLeastOnce`.

The `bufferSize` sets the maximum number of messages read from MQTT before back-pressure applies.

Scala
: @@snip [snip](/mqtt/src/test/scala/docs/scaladsl/MqttSourceSpec.scala) { #create-source-with-manualacks }

Java
: @@snip [snip](/mqtt/src/test/java/docs/javadsl/MqttSourceTest.java) { #create-source-with-manualacks }


The `atLeastOnce` source returns @scala[@scaladoc[MqttMessageWithAck](akka.stream.alpakka.mqtt.scaladsl.MqttMessageWithAck)]@java[@scaladoc[MqttMessageWithAck](akka.stream.alpakka.mqtt.javadsl.MqttMessageWithAck)] so you can acknowledge them by calling `ack()`.

Scala
: @@snip [snip](/mqtt/src/test/scala/docs/scaladsl/MqttSourceSpec.scala) { #run-source-with-manualacks }

Java
: @@snip [snip](/mqtt/src/test/java/docs/javadsl/MqttSourceTest.java) { #run-source-with-manualacks }


## Publishing to MQTT

To publish messages to the MQTT server create a sink be specifying `MqttConnectionSettings` (@scaladoc[API](akka.stream.alpakka.mqtt.MqttConnectionSettings$)) and a default Quality of Service-level.

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

The `bufferSize` sets the maximum number of messages read from MQTT before back-pressure applies.

Scala
: @@snip [snip](/mqtt/src/test/scala/docs/scaladsl/MqttFlowSpec.scala) { #create-flow }

Java
: @@snip [snip](/mqtt/src/test/java/docs/javadsl/MqttFlowTest.java) { #create-flow }


Run the flow by connecting a source of messages to be published and a sink for received messages.

Scala
: @@snip [snip](/mqtt/src/test/scala/docs/scaladsl/MqttFlowSpec.scala) { #run-flow }

Java
: @@snip [snip](/mqtt/src/test/java/docs/javadsl/MqttFlowTest.java) { #run-flow }


## Using flow with Acknowledge on message sent

It is possible to create a flow that receive `MqttMessageWithAck` instead `MqttMessage`.
In this case, when the message is successfully sent to the broker, an ack in sent.
This flow can be used in a stream when the source must be acknowledged ONLY when the message is sent.

The flow receive a `MqttMessageWithAck` with the message swapped with the new content and the ack function from source

Scala
: @@snip [snip](/mqtt/src/test/scala/docs/scaladsl/MqttFlowSpec.scala) { #create-flow-ack }

Java
: @@snip [snip](/mqtt/src/test/java/docs/javadsl/MqttFlowTest.java) { #create-flow-ack }

Run the flow by connecting a source of messages to be published and a sink for received messages.
When the message are sent, an ack is called.

Scala
: @@snip [snip](/mqtt/src/test/scala/docs/scaladsl/MqttFlowSpec.scala) { #run-flow-ack }

Java
: @@snip [snip](/mqtt/src/test/java/docs/javadsl/MqttFlowTest.java) { #run-flow-ack }

## Capturing MQTT client logging

The Paho library uses its own logging adapter and contains a default implementation to use `java.util.logging`. See [Paho/Log and Debug](https://wiki.eclipse.org/Paho/Log_and_Debug_in_the_Java_client).


## Running the example code

The code in this guide is part of runnable tests of this project. You are welcome to edit the code and run it in sbt.

> Test code requires a MQTT server running in the background. You can start one quickly using docker:
>
> `docker-compose up mqtt`

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
