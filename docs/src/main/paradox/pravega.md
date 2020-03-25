# Pravega

[Pravega](http://www.pravega.io/) provides a new storage abstraction - a stream - for continuous and unbounded data. A Pravega stream is a durable, elastic, append-only, unbounded sequence of bytes that has good performance and strong consistency.

@@project-info{ projectId="pravega" }

## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-pravega_$scala.binary.version$
  version=$project.version$
}

The table below shows direct dependencies of this module and the second tab shows all libraries it depends on transitively.

@@dependencies { projectId="pravega" }


## Concept

Pravega store at scale @extref[stream](pravega:pravega-concepts/#streams) of @extref[events](pravega:pravega-concepts/#events) organized within scopes.   

Basically @extref[events](pravega:pravega-concepts/#streams) are appended in and read from @extref[streams](pravega:pravega-concepts/#streams).

Pravega provides a durable storage with an unified API to access to cold events.

When messages are ingested, a *routing key* can be used to insure @extref[ordering guarantees](pravega:/pravega-concepts/#ordering-guarantees) at scale.  

For more information about [Pravega](http://www.pravega.io/) please visit the official @extref[documentation](pravega:/).

## Configuration

Two categories of properties can/must be provided to configure the connector.

**Pravega internals** properties that are forwarded to Pravega configuration builders:

  - @javadoc[ClientConfig](io.pravega.client.ClientConfig)  `akka.alpakka.pravega.defaults.client-config`
  - @javadoc[EventWriterConfig](io.pravega.client.stream.EventWriterConfig) `akka.alpakka.pravega.writer.config`
  - @javadoc[ReaderConfig](io.pravega.client.stream.ReaderConfig) `akka.alpakka.pravega.reader.config`

**Alpakka Connector** properties (all others).

reference.conf
: @@snip(/pravega/src/main/resources/reference.conf)

The Pravega connector can automatically configure the Pravega client by supplying Lightbend configuration in an
application.conf, or it can be set programmatically with @apidoc[ReaderSettingsBuilder$] or @apidoc[WriterSettingsBuilder$].
See the following sections for examples.

### ClientConfig

This configuration holds connection properties (endpoints, protocol) 
for all communication.

It can be overridden in an `application.conf` file at the following configuration paths:

 - reader: `akka.alpakka.pravega.reader.client-config`
 - writer: `akka.alpakka.pravega.writer.client-config` 

It can be customised programmatically, see below.

### EventReader configuration

A Pravega Source needs a @apidoc[ReaderSettings] to operate, it can be built from configuration and programmatically
customized:

Scala
:   @@snip[snip](/pravega/src/test/scala/docs/scaladsl/PravegaSettingsSpec.scala) { #reader-settings }

Java
:   @@snip[snip](/pravega/src/test/java/docs/javadsl/PravegaSettingsTestCase.java) { #reader-settings }

### EventWriter configuration

A Pravega Flow or Sink needs a @apidoc[WriterSettings] to operate, it can be build from configuration and
programmatically customized:

If you want to use a @extref[routing key](pravega:/pravega-concepts/#ordering-guarantees), you need to provide a key extractor to @apidoc[WriterSettingsBuilder] via @apidoc[withKeyExtractor](WriterSettingsBuilder){ scala="#withKeyExtractor" java="#withKeyExtractor" } for your message type Scala.

Scala
:   @@snip[snip](/pravega/src/test/scala/docs/scaladsl/PravegaSettingsSpec.scala) { #writer-settings }

Java
:   @@snip[snip](/pravega/src/test/java/docs/javadsl/PravegaSettingsTestCase.java) { #writer-settings }


ReaderSettingsBuilder, ReaderSettingsBuilder produce respectively ReaderSettings and ReaderSettings once a
@javadoc[serializer](io.pravega.client.stream.Serializer) is provided.

## Writing to Pravega

Pravega message writes are done through a Flow/Sink like:

Scala
:   @@snip[snip](/pravega/src/test/scala/docs/scaladsl/PravegaReadWriteDocs.scala) { #writing }

Java
:   @@snip[snip](/pravega/src/test/java/docs/javadsl/PravegaReadWriteDocs.java) { #writing }

## Reading from Pravega

Pravega message reads are from a Source:  

Scala
:   @@snip[snip](/pravega/src/test/scala/docs/scaladsl/PravegaReadWriteDocs.scala) { #reading }

Java
:   @@snip[snip](/pravega/src/test/java/docs/javadsl/PravegaReadWriteDocs.java) { #reading }

It produces a stream of @apidoc[PravegaEvent], a thin wrapper which includes some Pravega metadata along with the
message.
