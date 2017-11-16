# Server-sent Events (SSE) Connector

The SSE connector provides a continuous source of server-sent events recovering from connection failure.

## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-sse_$scalaBinaryVersion$
  version=$version$
}

## Usage

Simply define an `EventSource` by giving a URI, a function for sending HTTP requests and an optional initial value for Last-Evend-ID header:  

Scala
: @@snip ($alpakka$/sse/src/test/scala/akka/stream/alpakka/sse/scaladsl/EventSourceSpec.scala) { #event-source }

Then happily consume `ServerSentEvent`s:

Scala
: @@snip ($alpakka$/sse/src/test/scala/akka/stream/alpakka/sse/scaladsl/EventSourceSpec.scala) { #consume-events }
