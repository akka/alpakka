# Server-sent Events (SSE)

The SSE connector provides a continuous source of server-sent events recovering from connection failure.

@@project-info{ projectId="sse" }

## Artifacts

@@@note
The Akka dependencies are available from Akka’s secure library repository. To access them you need to use a secure, tokenized URL as specified at https://account.akka.io/token.
@@@

Additionally, add the dependencies as below.

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-sse_$scala.binary.version$
  version=$project.version$
  symbol2=AkkaVersion
  value2=$akka.version$
  group2=com.typesafe.akka
  artifact2=akka-stream_$scala.binary.version$
  version2=AkkaVersion
  symbol3=AkkaHttpVersion
  value3=$akka-http.version$
  group3=com.typesafe.akka
  artifact3=akka-http_$scala.binary.version$
  version3=AkkaHttpVersion
}

The table below shows direct dependencies of this module and the second tab shows all libraries it depends on transitively.

@@dependencies { projectId="sse" }


## Usage

Define an `EventSource` by giving a URI, a function for sending HTTP requests, and an optional initial value for Last-Event-ID header:  

Scala
: @@snip [snip](/sse/src/test/scala/docs/scaladsl/EventSourceSpec.scala) { #event-source }

Java
: @@snip [snip](/sse/src/test/java/docs/javadsl/EventSourceTest.java) { #event-source }


Then happily consume `ServerSentEvent`s:

Scala
: @@snip [snip](/sse/src/test/scala/docs/scaladsl/EventSourceSpec.scala) { #consume-events }

Java
: @@snip [snip](/sse/src/test/java/docs/javadsl/EventSourceTest.java) { #consume-events }
