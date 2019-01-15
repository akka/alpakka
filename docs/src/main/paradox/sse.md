# Server-sent Events (SSE)

The SSE connector provides a continuous source of server-sent events recovering from connection failure.

@@project-info{ projectId="sse" }

## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-sse_$scala.binary.version$
  version=$project.version$
}

The table below shows direct dependencies of this module and the second tab shows all libraries it depends on transitively.

@@dependencies { projectId="sse" }


## Usage

Define an `EventSource` by giving a URI, a function for sending HTTP requests, and an optional initial value for Last-Evend-ID header:  

Scala
: @@snip [snip](/sse/src/test/scala/docs/scaladsl/EventSourceSpec.scala) { #event-source }

Java
: @@snip [snip](/sse/src/test/java/docs/javadsl/EventSourceTest.java) { #event-source }


Then happily consume `ServerSentEvent`s:

Scala
: @@snip [snip](/sse/src/test/scala/docs/scaladsl/EventSourceSpec.scala) { #consume-events }

Java
: @@snip [snip](/sse/src/test/java/docs/javadsl/EventSourceTest.java) { #consume-events }
