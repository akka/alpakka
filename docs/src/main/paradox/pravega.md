# Pravega

[Pravega](https://www.pravega.io/) provides a new storage abstraction - a stream - for continuous and unbounded data. 
A Pravega stream is an elastic set of durable and append-only segments, each segment being an unbounded sequence of bytes. 
Streams provide exactly-once semantics, and atomicity for groups of events using transactions.

@@project-info{ projectId="pravega" }

## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-pravega_$scala.binary.version$
  version=$project.version$
}

The table below shows direct dependencies of this module and the second tab shows all libraries it depends on transitively.

@@dependencies { projectId="pravega" }


## Concepts

Pravega stores streams of @extref[events](pravega:pravega-concepts/#events), and @extref[streams](pravega:pravega-concepts/#streams) are organized using scopes. 
A Pravega stream comprises a one or more parallel segments, and the set of parallel segments can change over time with auto-scaling. 
Pravega is designed to operate at scale and is able to accommodate a large number of segments and streams.

Pravega has an API to write and read @extref[events](pravega:pravega-concepts/#events). 
An application looking into ingesting data writes events to a @extref[stream](pravega:pravega-concepts/#streams), while consuming data consists of reading events from a stream. 
In addition to the events API, Pravega has other APIs that enable an application to read and write bytes rather than events and to read events of a stream out of order (e.g., when batch processing).

Pravega stores stream data durably, and applications can access the stream data using the same API both when tailing the stream and when processing past data. 
The system is architected so that the underlying storage is elastic and it is able to accommodate unbounded streams.

When writing an event, Pravega accepts a *routing key* parameter, and it @extref[guarantees order](pravega:/pravega-concepts/#ordering-guarantees) per key even in the presence of auto-scaling.

For more information about [Pravega](https://www.pravega.io/) please visit the official @extref[documentation](pravega:/).

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

A Pravega Flow or Sink needs a @apidoc[WriterSettings] to operate, it can be built from configuration and programmatically customized:

You may want to use a @extref[routing key](pravega:/pravega-concepts/#ordering-guarantees), you have to provide a @apidoc[key extractor function](WriterSettingsBuilder){ scala="#withKeyExtractor" java="#withKeyExtractor" } for your message type.

Scala
:   @@snip[snip](/pravega/src/test/scala/docs/scaladsl/PravegaSettingsSpec.scala) { #writer-settings }

Java
:   @@snip[snip](/pravega/src/test/java/docs/javadsl/PravegaSettingsTestCase.java) { #writer-settings }


@apidoc[ReaderSettingsBuilder$], @apidoc[ReaderSettingsBuilder] produce respectively ReaderSettings and ReaderSettings once a
@javadoc[Serializer](io.pravega.client.stream.Serializer) is provided.

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

## Support

In addition to our regular Alpakka community support on [![gitter: akka/akka](https://img.shields.io/badge/gitter%3A-akka%2Fakka-blue.svg?style=flat-square)](https://gitter.im/akka/akka) and Lightbend's [discuss.lightbend.com](https://discuss.lightbend.com/c/akka/streams-and-alpakka), you can also visit the `#akka-streams-connector` channel on the [Pravega slack](https://pravega-slack-invite.herokuapp.com) for assistance with Pravega integration itself.
