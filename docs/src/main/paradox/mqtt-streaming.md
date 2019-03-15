# MQTT Streaming

@@@ note { title="MQTT Streaming" }

MQTT stands for MQ Telemetry Transport. It is a publish/subscribe messaging protocol, designed for constrained devices and low-bandwidth, high-latency or unreliable networks. The design principles are to minimize network bandwidth and device resource requirements whilst also attempting to ensure reliability and some degree of assurance of delivery. These principles also turn out to make the protocol ideal of the emerging “machine-to-machine” (M2M) or “Internet of Things” world of connected devices, and for mobile applications where bandwidth and battery power are at a premium.  

Further information on [mqtt.org](https://mqtt.org/).

@@@ 

@@@ note { title="Paho Differences" }

Alpakka contains @ref[another MQTT connector](mqtt.md) which is based on the Eclipse Paho client. Unlike the Paho version, this library has no dependencies other than those of Akka Streams i.e. it is entirely reactive. As such, there should be a significant performance advantage given its pure-Akka foundations in terms of memory usage given its diligent use of threads.

This library also differs in that it separates out the concern of how MQTT is connected. Unlike Paho, where TCP is assumed, this library can join in any flow. The end result is that by using this library, Unix Domain Sockets, TCP, UDP or anything else can be used to transport MQTT.

@@@

The Alpakka MQTT Streaming connector provides an Akka Stream flow to connect to MQTT brokers. In addition, a flow is provided so that you can implement your own MQTT server in the case where you do not wish to use a broker--MQTT is a fine protocol for directed client/server interactions, as well as having an intermediary broker.

Alpakka MQTT Streaming implements the [MQTT 3.1.1](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html) protocol.

@@project-info{ projectId="mqtt-streaming" }

## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-mqtt-streaming_$scala.binary.version$
  version=$project.version$
  symbol2=AkkaVersion
  value2=$akka26.version$
  group2=com.typesafe.akka
  artifact2=akka-stream_$scala.binary.version$
  version2=AkkaVersion
  group3=com.typesafe.akka
  artifact3=akka-actor-typed_$scala.binary.version$
  version3=AkkaVersion
}

The table below shows direct dependencies of this module and the second tab shows all libraries it depends on transitively.

@@dependencies { projectId="mqtt-streaming" }

@@@ note

Unlike most Alpakka modules, mqtt-streaming requires at least Akka $akka26.version$.

@@@

## Subscribing to MQTT topics

@@@warning { title="API may change" }

The high-level API is experimental and may be changed even in minor versions.

@@@


To subscribe to MQTT topics this connector offers a high-level API wrapping the more involved use of the underlying API which offers all flexibility.

The @scala[@scaladoc[`MqttClientSession`](akka.stream.alpakka.mqtt.streaming.scaladsl.MqttClientSession)]@java[@scaladoc[`MqttClientSession`](akka.stream.alpakka.mqtt.streaming.javadsl.MqttClientSession)] can be shared between clients and should be shut down after all streams are completed.

### At-least-once with high-level API

The high-level @scala[@scaladoc[`MqttSource`](akka.stream.alpakka.mqtt.streaming.scaladsl.MqttSource$)]@java[@scaladoc[`MqttSource`](akka.stream.alpakka.mqtt.streaming.javadsl.MqttSource$)] at-most-once and at-least-once sources. The at-least-once source emits @scala[@scaladoc[`MqttAckHandle`](akka.stream.alpakka.mqtt.streaming.scaladsl.MqttAckHandle)]@java[@scaladoc[`MqttSource`](akka.stream.alpakka.mqtt.streaming.javadsl.MqttAckHandle)] which offers the `ack()` method.

Scala
: @@snip [snip](/mqtt-streaming/src/test/scala/docs/scaladsl/MqttSourceSpec.scala) { #imports #at-least-once }


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

Note that the `Publish` command is not offered to the command flow given MQTT QoS requirements. Instead, the 
session is told to perform `Publish` given that it can retry continuously with buffering until a command 
flow is established.

We filter the events received as there will be ACKs to our connect, subscribe and publish. The collected event
is the publication to the topic we just subscribed to.

To shut down the flow after use, the command queue `commands` is completed and after its completion the `session` is shut down.

## Flow through a server session

The following code illustrates how to establish an MQTT server session and join it with a TCP binding:

Scala
: @@snip [snip](/mqtt-streaming/src/test/scala/docs/scaladsl/MqttFlowSpec.scala) { #create-streaming-bind-flow }

Java
: @@snip [snip](/mqtt-streaming/src/test/java/docs/javadsl/MqttFlowTest.java) { #create-streaming-bind-flow }

The resulting source's type shows how `Event`s are received and `Command`s are queued in reply. Our example
acknowledges a connection, subscription and publication. Upon receiving a publication, it is re-published
from the server so that any client that is subscribed will receive it. An additional detail is that we hold
off re-publishing until we have a subscription from the client. Note also how the session is told to perform
`Publish` commands directly as they will be broadcasted to all clients subscribed to the topic.

Run the flow:

Scala
: @@snip [snip](/mqtt-streaming/src/test/scala/docs/scaladsl/MqttFlowSpec.scala) { #run-streaming-bind-flow }

Java
: @@snip [snip](/mqtt-streaming/src/test/java/docs/javadsl/MqttFlowTest.java) { #run-streaming-bind-flow }

To shut down the server after use, the server flow is shut down via a `KillSwitch` and the `session` is shut down.
