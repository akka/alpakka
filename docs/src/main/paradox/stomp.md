# STOMP Protocol Connector

The Stomp Protocol connector provides Akka Stream sources and sinks to connect to STOMP servers.

## Reported issues

[Tagged issues at Github](https://github.com/akka/alpakka/labels/p%3Astomp)


## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-stomp_$scalaBinaryVersion$
  version=$version$
}

## Usage

### Connecting to a STOMP server

All the STOMP connectors are configured using a @scaladoc[ConnectionProvider](akka.stream.alpakka.stomp.ConnectionProvider).

There are some types of @scaladoc[ConnectionProvider](akka.stream.alpakka.stomp.ConnectionProvider):

* @scaladoc[LocalConnectionProvider](akka.stream.alpakka.stomp.LocalConnectionProvider) which connects to the default localhost. It creates a new connection for each stage.
* @scaladoc[DetailsConnectionProvider](akka.stream.alpakka.stomp.DetailsConnectionProvider) which supports more fine-grained configuration. It creates a new connection for each stage.

### Sinking messages into a STOMP server

Create the ConnectorSettings

Scala
: @@snip ($alpakka$/stomp/src/test/scala/akka/stream/alpakka/stomp/scaladsl/StompClientSinkSpec.scala) { #connector-settings }

@scaladoc[StompClientSink](akka.stream.alpakka.stomp.scaladsl.StompClientSink) is a collection of factory methods that facilitates creation of sinks. 

Create a sink, that accepts and forwards @scaladoc[SendingFrame](akka.stream.alpakka.stomp.client.SendingFrame)s to the STOMP server.

Scala
: @@snip ($alpakka$/stomp/src/test/scala/akka/stream/alpakka/stomp/scaladsl/StompClientSinkSpec.scala) { #stomp-client-sink }

Last step is to @extref[materialize](akka-docs:scala/stream/stream-flows-and-basics) and run the sink we created.

Scala
: @@snip ($alpakka$/stomp/src/test/scala/akka/stream/alpakka/stomp/scaladsl/StompClientSinkSpec.scala) { #stomp-client-sink-materialization }

### Receiving messages from STOMP server using a StompClientSource

Create the [ConnectorSettings] that specifies the STOMP server to connect, and the STOMP `destination` that you want receive messages from

: @@snip ($alpakka$/stomp/src/test/scala/akka/stream/alpakka/stomp/scaladsl/StompClientSourceSpec.scala) { #connector-settings }

Create a source, that generates @scaladoc[SendingFrame](akka.stream.alpakka.stomp.client.SendingFrame$)

: @@snip ($alpakka$/stomp/src/test/scala/akka/stream/alpakka/stomp/scaladsl/StompClientSourceSpec.scala) { #stomp-client-source }

Last step is to @extref[materialize](akka-docs:scala/stream/stream-flows-and-basics) and run the source we created.

: @@snip ($alpakka$/stomp/src/test/scala/akka/stream/alpakka/stomp/scaladsl/StompClientSourceSpec.scala) { #materialization }



### Running the example code

The code in this guide is part of runnable tests of this project. You are welcome to edit the code and run it in sbt.

Test code does not require an STOMP server running in the background, since it creates one per test using Vertx Stomp library. 

Scala
:   ```
    sbt
    > stomp/testOnly *.StompClientSourceSpec
    ```






