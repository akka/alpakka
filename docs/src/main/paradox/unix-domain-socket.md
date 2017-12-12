# Unix Domain Socket Connector

[From Wikipedia](https://en.wikipedia.org/wiki/Unix_domain_socket), _A Unix domain socket or IPC socket (inter-process communication socket) is a data communications endpoint for exchanging data between processes executing on the same host operating system._ Unix Domain Sockets leverage files and so operating system level access control can be utilized. This is a security advantage over using TCP/UDP where IPC is required without a more complex [Transport Layer Security (TLS)](https://en.wikipedia.org/wiki/Transport_Layer_Security). Performance also favors Unix Domain Sockets over TCP/UDP given that the Operating System's network stack is bypassed.

This connector provides an implementation of a Unix Domain Socket with interfaces modelled on the conventional `Tcp` Akka Streams class. The connector uses JNI and so there are no native dependencies.

> Note that Unix Domain Sockets, as the name implies, do not apply to Windows.

## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-unixdomainsocket_$scalaBinaryVersion$
  version=$version$
}

## Usage

The binding and connecting APIs are extremely similar to the `Tcp` Akka Streams class. `UnixDomainSocket` is generally substitutable for `Tcp` except that the `SocketAddress` is different (Unix Domain Sockets requires a `java.net.File` as opposed to a host and port). Please read the following for details:

* [Scala user reference for `Tcp`](https://doc.akka.io/docs/akka/current/stream/stream-io.html?language=scala)
* [Java user reference for `Tcp`](https://doc.akka.io/docs/akka/current/stream/stream-io.html?language=java)

### Binding to a file

Scala
: @@snip ($alpakka$/unix-domain-socket/src/test/scala/akka/stream/alpakka/unixdomainsocket/scaladsl/UnixDomainSocketSpec.scala) { #binding }

Java
: @@snip ($alpakka$/unix-domain-socket/src/test/java/akka/stream/alpakka/unixdomainsocket/javadsl/UnixDomainSocketTest.java) { #binding }

### Connecting to a file

Scala
: @@snip ($alpakka$/unix-domain-socket/src/test/scala/akka/stream/alpakka/unixdomainsocket/scaladsl/UnixDomainSocketSpec.scala) { #outgoingConnection }

Java
: @@snip ($alpakka$/unix-domain-socket/src/test/java/akka/stream/alpakka/unixdomainsocket/javadsl/UnixDomainSocketTest.java) { #outgoingConnection }

