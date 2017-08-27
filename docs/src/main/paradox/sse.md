# Server-sent Events (SSE) Connector

The SSE connector provides a continuous source of server-sent events recovering from connection failure.

## Artifacts

sbt
:   @@@vars
    ```scala
    libraryDependencies += "com.lightbend.akka" %% "akka-stream-alpakka-sse" % "$version$"
    ```
    @@@

Maven
:   @@@vars
    ```xml
    <dependency>
      <groupId>com.lightbend.akka</groupId>
      <artifactId>akka-stream-alpakka-sse_$scalaBinaryVersion$</artifactId>
      <version>$version$</version>
    </dependency>
    ```
    @@@

Gradle
:   @@@vars
    ```gradle
    dependencies {
      compile group: "com.lightbend.akka", name: "akka-stream-alpakka-sse_$scalaBinaryVersion$", version: "$version$"
    }
    ```
    @@@

## Usage

Simply define an `EventSource` by giving a URI, a function for sending HTTP requests and an optional initial value for Last-Evend-ID header:  

Scala
: @@snip (../../../../sse/src/test/scala/akka/stream/alpakka/sse/scaladsl/EventSourceSpec.scala) { #event-source }

Then happily consume `ServerSentEvent`s:

Scala
: @@snip (../../../../sse/src/test/scala/akka/stream/alpakka/sse/scaladsl/EventSourceSpec.scala) { #consume-events }
