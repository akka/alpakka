# Apache Geode

[Apache Geode](https://geode.apache.org) is a distributed datagrid (formerly called ["Gemfire" which used to be Pivotal's packaging of Geode and now is VMware Tanzu](https://tanzu.vmware.com/gemfire)).

Alpakka Geode provides flows and sinks to put elements into Geode, and a source to retrieve elements from it. It stores key-value-pairs. Keys and values must be serialized with Geode's support for it.

@@project-info{ projectId="geode" }

## Artifacts

The Akka dependencies are available from Akka's library repository. To access them there, you need to configure the URL for this repository.

@@repository [sbt,Maven,Gradle] {
id="akka-repository"
name="Akka library repository"
url="https://repo.akka.io/maven"
}

Additionally, add the dependencies as below.

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-geode_$scala.binary.version$
  version=$project.version$
  symbol2=AkkaVersion
  value2=$akka.version$
  group2=com.typesafe.akka
  artifact2=akka-stream_$scala.binary.version$
  version2=AkkaVersion
}

The table below shows direct dependencies of this module and the second tab shows all libraries it depends on transitively.

@@dependencies { projectId="geode" }

## Setup

### Connection

The connection to Geode is handled by a @extref[ClientCache](geode:basic_config/the_cache/managing_a_client_cache.html). A single  `ClientCache` per application is enough. `ClientCache` also holds a single `PDXSerializer`.

The Geode client should be closed after use, it is recommended to close it on actor system termination.

Scala
: @@snip [snip](/geode/src/test/scala/docs/scaladsl/GeodeFlowSpec.scala) { #connection }

Java
: @@snip [snip](/geode/src/test/java/docs/javadsl/GeodeBaseTestCase.java) { #connection }

Apache Geode supports continuous queries. Continuous query rely on server events, thus Alpakka Geode needs to listen to those events. This behaviour -- as it consumes more resources  -- is isolated in a Scala trait and/or an specialized Java class.

Scala
: @@snip [snip](/geode/src/test/scala/docs/scaladsl/GeodeContinuousSourceSpec.scala) { #connection-with-pool }

Java
: @@snip [snip](/geode/src/test/java/docs/javadsl/GeodeBaseTestCase.java) { #connection-with-pool }

### Region

Define a @extref[region](geode:/basic_config/data_regions/chapter_overview.html) setting to describe how to access region and the key extraction function.

Scala
: @@snip [snip](/geode/src/test/scala/docs/scaladsl/GeodeBaseSpec.scala) { #region }

Java
: @@snip [snip](/geode/src/test/java/docs/javadsl/GeodeBaseTestCase.java) { #region }


### Serialization

Objects must be serialized to be stored in or retrieved from Geode. Only **PDX format** is available with Alpakka Geode.
`PDXEncoder`s support many options as described in @extref[Geode PDX Serialization](geode:/developing/data_serialization/gemfire_pdx_serialization.html).
A `PdxSerializer` must be provided to Geode when reading from or writing to a region.

Scala
:   @@snip [snip](/geode/src/test/scala/docs/scaladsl/PersonPdxSerializer.scala) { #person-pdx-serializer }

Java
:   @@snip [snip](/geode/src/test/java/docs/javadsl/PersonPdxSerializer.java) { #person-pdx-serializer }


This Alpakka Geode provides a generic solution for Scala users based on [Shapeless](https://github.com/milessabin/shapeless) which may generate serializers for case classes at compile time.

Java users need to implement custom serializers manually, or use runtime reflection as described in @extref[Using Automatic Reflection-Based PDX Serialization](geode:/developing/data_serialization/auto_serialization.html).


## Writing to Geode

This example stores data in Geode within a flow.

Scala
: @@snip [snip](/geode/src/test/scala/docs/scaladsl/GeodeFlowSpec.scala) { #flow }

Java
: @@snip [snip](/geode/src/test/java/docs/javadsl/GeodeFlowTestCase.java) { #flow }


This example stores data in Geode by using a sink.

Scala
: @@snip [snip](/geode/src/test/scala/docs/scaladsl/GeodeSinkSpec.scala) { #sink }

Java
: @@snip [snip](/geode/src/test/java/docs/javadsl/GeodeSinkTestCase.java) { #sink }


## Reading from Geode

### Simple query

Apache Geode supports simple queries.

Scala
: @@snip [snip](/geode/src/test/scala/docs/scaladsl/GeodeFiniteSourceSpec.scala) { #query }

Java
: @@snip [snip](/geode/src/test/java/docs/javadsl/GeodeFiniteSourceTestCase.java) { #query }


### Continuous query

Continuous queries need to be explicitly closed, to connect creating and closing a unique identifier needs to be passed to both `continuousQuery` and `closeContinuousQuery`.

Scala
: @@snip [snip](/geode/src/test/scala/docs/scaladsl/GeodeContinuousSourceSpec.scala) { #continuousQuery }

Java
: @@snip [snip](/geode/src/test/java/docs/javadsl/GeodeContinuousSourceTestCase.java) { #continuousQuery }


## Geode basic commands

Assuming Apache Geode is installed:

```
gfsh
```

From the Geode shell:

```
start locator --name=locator
configure pdx --read-serialized=true
start server --name=server

create region --name=animals --type=PARTITION_REDUNDANT --redundant-copies=2
create region --name=persons --type=PARTITION_REDUNDANT --redundant-copies=2

```
