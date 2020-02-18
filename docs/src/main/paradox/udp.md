# UDP

The UDP connector provides Akka Stream flows that allow to send and receive UDP datagrams.

@@project-info{ projectId="udp" }

## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-udp_$scala.binary.version$
  version=$project.version$
  symbol2=AkkaVersion
  value2=$akka.version$
  group2=com.typesafe.akka
  artifact2=akka-stream_$scala.binary.version$
  version2=AkkaVersion
}

The table below shows direct dependencies of this module and the second tab shows all libraries it depends on transitively.

@@dependencies { projectId="udp" }


## Sending

Datagrams can be sent to remote destinations by using a `Udp.sendFlow` or `Udp.sendSink` which can be found in the
@scaladoc[Udp](akka.stream.alpakka.udp.scaladsl.Udp$) factory object.

Scala
: @@snip [snip](/udp/src/test/scala/docs/scaladsl/UdpSpec.scala) { #send-datagrams }

Java
: @@snip [snip](/udp/src/test/java/docs/javadsl/UdpTest.java) { #send-datagrams }

## Receiving

First create an address which will be used to bind and listen for incoming datagrams.

Scala
: @@snip [snip](/udp/src/test/scala/docs/scaladsl/UdpSpec.scala) { #bind-address }

Java
: @@snip [snip](/udp/src/test/java/docs/javadsl/UdpTest.java) { #bind-address }

A Flow created from `Udp.bindFlow` will bind to the given address. All datagrams coming from the network
to the bound address will be sent downstream. Datagrams received from the upstream will be sent to their
corresponding destination addresses.

The flow materializes to the @scala[`Future[InetSocketAddress]`]@java[`CompletionStage<InetSocketAddress>`] which
will eventually hold the address the flow was finally bound to.

Scala
: @@snip [snip](/udp/src/test/scala/docs/scaladsl/UdpSpec.scala) { #bind-flow }

Java
: @@snip [snip](/udp/src/test/java/docs/javadsl/UdpTest.java) { #bind-flow }

## Running the example code

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
