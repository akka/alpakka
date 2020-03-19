# Apache Cassandra

@@@ note { title="Cassandra"}

Apache Cassandra is a free and open-source, distributed, wide column store, NoSQL database management system designed to handle large amounts of data across many commodity servers, providing high availability with no single point of failure. Cassandra offers robust support for clusters spanning multiple datacenters, with asynchronous masterless replication allowing low latency operations for all clients.

-- [Wikipedia](https://en.wikipedia.org/wiki/Apache_Cassandra)

@@@

Alpakka Cassandra offers an @extref:[Akka Streams](akka:/streams/index.html) API on top of a @javadoc[CqlSession](com.datastax.oss.driver.api.core.CqlSession) from the @extref:[Datastax Java Driver](cassandra-driver:) version 4.0+. The @ref:[driver configuration](#custom-session-creation) is provided in the same config format as Akka uses and can be placed in the same `application.conf` as your Akka settings.

@@project-info{ projectId="cassandra" }

## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-cassandra_$scala.binary.version$
  version=$project.version$
}

The table below shows direct dependencies of this module and the second tab shows all libraries it depends on transitively.

@@dependencies { projectId="cassandra" }


## Sessions

Cassandra is accessed through @apidoc[CassandraSession]s which are managed by the @apidoc[CassandraSessionRegistry] Akka extension. This way a session is shared across all usages within the actor system and properly shut down after the actor system is shut down.

@scala[The `CassandraSession` is provided to the stream factory methods as an `implicit` parameter.]

Scala
: @@snip [snip](/cassandra/src/test/scala/docs/scaladsl/CassandraSourceSpec.scala) { #init-session }

Java
: @@snip [snip](/cassandra/src/test/java/docs/javadsl/CassandraSourceTest.java) { #init-session }

See @ref[custom session creation](#custom-session-creation) below for tweaking this.


## Reading from Cassandra

@apidoc[CassandraSource] provides factory methods to get Akka Streams Sources from CQL queries and from @javadoc[com.datastax.oss.driver.api.core.cql.Statement](com.datastax.oss.driver.api.core.cql.Statement)s.

Dynamic parameters can be provided to the CQL as variable arguments.

Scala
: @@snip [snip](/cassandra/src/test/scala/docs/scaladsl/CassandraSourceSpec.scala) { #cql }

Java
: @@snip [snip](/cassandra/src/test/java/docs/javadsl/CassandraSourceTest.java) { #cql }


If the statement requires specific settings, you may pass any @javadoc[com.datastax.oss.driver.api.core.cql.Statement](com.datastax.oss.driver.api.core.cql.Statement).

Scala
: @@snip [snip](/cassandra/src/test/scala/docs/scaladsl/CassandraSourceSpec.scala) { #statement }

Java
: @@snip [snip](/cassandra/src/test/java/docs/javadsl/CassandraSourceTest.java) { #statement }


Here we used a basic sink to complete the stream by collecting all of the stream elements into a collection. The power of streams comes from building larger data pipelines which leverage backpressure to ensure efficient flow control. Feel free to edit the example code and build @extref:[more advanced stream topologies](akka:stream/stream-introduction.html).


## Writing to Cassandra

@apidoc[CassandraFlow] provides factory methods to get Akka Streams flows to run CQL statements that change data (`UPDATE`, `INSERT`). Alpakka Cassandra creates a @javadoc[PreparedStatement](com.datastax.oss.driver.api.core.cql.PreparedStatement) and for every stream element the `statementBinder` function binds the CQL placeholders to data.

The incoming elements are emitted unchanged for further processing.

Scala
: @@snip [snip](/cassandra/src/test/scala/docs/scaladsl/CassandraFlowSpec.scala) { #prepared }

Java
: @@snip [snip](/cassandra/src/test/java/docs/javadsl/CassandraFlowTest.java) { #prepared }

### Update flows with context

Alpakka Cassandra flows offer **"With Context"**-support which integrates nicely with some other Alpakka connectors.

Scala
: @@snip [snip](/cassandra/src/test/scala/docs/scaladsl/CassandraFlowSpec.scala) { #withContext }

Java
: @@snip [snip](/cassandra/src/test/java/docs/javadsl/CassandraFlowTest.java) { #withContext }


## Custom Session creation

Session creation and configuration is controlled via settings in `application.conf`. The @apidoc[CassandraSessionSettings] accept a full path to a configuration section which needs to specify a `session-provider` setting. The @apidoc[CassandraSessionRegistry] expects a fully qualified class name to a class implementing @apidoc[CqlSessionProvider].

Alpakka Cassandra includes a default implementation @apidoc[DefaultSessionProvider], which is referenced in the default configuration `alpakka.cassandra`.

The @apidoc[DefaultSessionProvider] config section must contain:

* a settings section `service-discovery` which may be used to discover Cassandra contact points via @ref:[Akka Discovery](#using-akka-discovery),
* and a reference to a Cassandra config section in `datastax-java-driver-config` which is used to configure the Cassandra client. For details see the @extref:[Datastax Java Driver configuration](cassandra-driver:manual/core/configuration/#quick-overview) and the driver's @extref:[`reference.conf`](cassandra-driver:manual/core/configuration/reference/).

reference.conf
: @@snip [snip](/cassandra/src/main/resources/reference.conf)

In simple cases your `datastax-java-driver` section will need to define `contact-points` and `load-balancing-policy.local-datacenter`. To make the Cassandra driver retry its initial connection attempts, add `advanced.reconnect-on-init = true`.

application.conf
: @@snip [snip](/cassandra/src/test/resources/application.conf) { #datastax-sample }


### Using Akka Discovery

To use @extref[Akka Discovery](akka:discovery/) make sure the `akka-discovery` dependency is on you classpath.

@@dependency [sbt,Maven,Gradle] {
  symbolAkka=AkkaVersion
  valueAkka="$akka.version$"
  group="com.typesafe.akka"
  artifact="akka-discovery_$scala.binary.version$"
  version=AkkaVersion
}

To enable @extref[Akka Discovery](akka:discovery/) with the @apidoc[DefaultSessionProvider], set up the desired service name in the discovery mechanism of your choice and pass that name in `service-discovery.name`. The example below extends the `alpakka.cassandra` config section and only overwrites the service name.

application.conf
: @@snip [snip](/cassandra/src/test/resources/application.conf) { #akka-discovery-docs }

Use the full config section path to create the @apidoc[CassandraSessionSettings$].

Scala
: @@snip [snip](/cassandra/src/test/scala/docs/scaladsl/AkkaDiscoverySpec.scala) { #discovery }

Java
: @@snip [snip](/cassandra/src/test/java/docs/javadsl/CassandraSourceTest.java) { #discovery }
