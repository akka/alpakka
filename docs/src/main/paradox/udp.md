# UDP

The UDP connector provides Akka Stream flows that allow to send and receive UDP messages.

### Reported issues

[Tagged issues at Github](https://github.com/akka/alpakka/labels/p%3Audp)

## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-udp_$scalaBinaryVersion$
  version=$version$
}

## Usage

### Sending

Messages can be sent to remote destinations by using a `Udp.sendFlow` or `Udp.sendSink` which can be found in the
@scaladoc[Udp](akka.stream.alpakka.udp.scaladsl.Udp$) factory object.

Scala
: @@snip ($alpakka$/udp/src/test/scala/akka/stream/alpakka/udp/UdpSpec.scala) { #send-messages }

Java
: @@snip ($alpakka$/udp/src/test/java/akka/stream/alpakka/udp/UdpTest.java) { #send-messages }

### Receiving

First create an address which will be used to bind and listen for incoming messages.

Scala
: @@snip ($alpakka$/udp/src/test/scala/akka/stream/alpakka/udp/UdpSpec.scala) { #bind-address }

Java
: @@snip ($alpakka$/udp/src/test/java/akka/stream/alpakka/udp/UdpTest.java) { #bind-address }

A Flow created from `Udp.bindFlow` will bind to the given address. All messages coming from the network
to the bound address will be sent downstream. Messages received from the upstream will be sent to their
corresponding destination addresses.

The flow materializes to the @scala[`Future[InetSocketAddress]`]@java[`CompletionStage[InetSocketAddress]`] which
will eventually hold the address the flow was finally bound to.

Scala
: @@snip ($alpakka$/udp/src/test/scala/akka/stream/alpakka/udp/UdpSpec.scala) { #bind-flow }

Java
: @@snip ($alpakka$/udp/src/test/java/akka/stream/alpakka/udp/UdpTest.java) { #bind-flow }

### Running the example code

The code in this guide is part of runnable tests of this project. You are welcome to browse the code, edit and run it in sbt.

Scala
:   ```
    sbt
    > udp/testOnly *.UdpSpec
    ```

Java
:   ```
    sbt
    > udp/testOnly *.UdpTest
    ```