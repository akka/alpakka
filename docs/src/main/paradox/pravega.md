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

Pravega store at scale stream of event organized within Scopes.   

http://pravega.io/docs/latest/terminology/

## Configuration

Two categories of properties can/must be provided to configure the connector.

**Pravega internals** properties that are forwarded to Pravega configuration builders:

  - ClientConfig  `akka.alpakka.pravega.defaults.client-config`
  - EventWriterConfig `akka.alpakka.pravega.writer.config`
  - ReaderConfig `akka.alpakka.pravega.reader.config`

**Alpakka Connector** properties (all others).

reference.conf
: @@snip(/pravega/src/main/resources/reference.conf)


Connectors can be configured by configuration or programmatically. To programmatically set the Pravega internals properties",
Builder are exposed by the connector setting builder.


### ClientConfig

This configuration hold connection properties (endpoints, protocol) 
for all communication.
It can be overridden by configuration file for:
 - reader: `akka.alpakka.pravega.reader.client-config`
 - writer: `akka.alpakka.pravega.writer.client-config` 

It can be customised programmatically, see below.

### EventReader configuration

A Pravega Source needs a ReaderSettings to operate, it can be build from configuration and programmatically customized:

Scala
:   @@snip[snip](/pravega/src/test/scala/docs/scaladsl/PravegaSettingsSpec.scala) { #reader-settings }

Java
:   @@snip[snip](/pravega/src/test/java/docs/javadsl/PravegaSettingsTestCase.java) { #reader-settings }

### EventWriter configuration

A Pravega Flow or Sink needs a WriterSettings to operate, it can be build from configuration and
programmatically customized:

If you want to use a routing key, you need to provide an extraction function from your messages.

Scala
:   @@snip[snip](/pravega/src/test/scala/docs/scaladsl/PravegaSettingsSpec.scala) { #writer-settings }

Java
:   @@snip[snip](/pravega/src/test/java/docs/javadsl/PravegaSettingsTestCase.java) { #writer-settings }


ReaderSettingsBuilder, ReaderSettingsBuilder produce respectively ReaderSettings and ReaderSettings
once a Serializer is provided.

## Writing to Pravega

Pravega write are done through a Flow/Sink like:

Scala
:   @@snip[snip](/pravega/src/test/scala/docs/scaladsl/PravegaReadWriteDocs.scala) { #writing }

Java
:   @@snip[snip](/pravega/src/test/java/docs/javadsl/PravegaReadWriteDocs.java) { #writing }


## Reading from Pravega

Pravega reads from a Source:  


Scala
:   @@snip[snip](/pravega/src/test/scala/docs/scaladsl/PravegaReadWriteDocs.scala) { #reading }

Java
:   @@snip[snip](/pravega/src/test/java/docs/javadsl/PravegaReadWriteDocs.java) { #reading }

It produces a stream of `PravegaEvent` a thin wrapper which embeds some Pravega metadata

