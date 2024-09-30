# Apache Kudu

@@@warning { title="End of life" }

The Kudu connector has not been updated for too long and is now considered End of Life. It will be removed with the next release of Alpakka.

@@@

The Alpakka Kudu connector supports writing to [Apache Kudu](https://kudu.apache.org) tables.

Apache Kudu is a free and open source column-oriented data store in the Apache Hadoop ecosystem.

@@project-info{ projectId="kudu" }


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
  artifact=akka-stream-alpakka-kudu_$scala.binary.version$
  version=$project.version$
  symbol2=AkkaVersion
  value2=$akka.version$
  group2=com.typesafe.akka
  artifact2=akka-stream_$scala.binary.version$
  version2=AkkaVersion
}

The table below shows direct dependencies of this module and the second tab shows all libraries it depends on transitively.

@@dependencies { projectId="kudu" }

## Configuration

To connect to Kudu you need:

1. Describe the Kudu @javadoc[Schema](org.apache.kudu.Schema)
1. Define a converter function to map your data type to a @javadoc[PartialRow](org.apache.kudu.client.PartialRow)
1. Specify Kudu @javadoc[CreateTableOptions](org.apache.kudu.client.CreateTableOptions)
1. Set up Alpakka's @scaladoc[KuduTableSettings](akka.stream.alpakka.kudu.KuduTableSettings)

Scala
:   @@snip [snip](/kudu/src/test/scala/docs/scaladsl/KuduTableSpec.scala) { #configure }

Java
:   @@snip [snip](/kudu/src/test/java/docs/javadsl/KuduTableTest.java) { #configure }

The @javadoc[KuduClient](org.apache.kudu.client.KuduClient) by default is automatically managed by the connector.
Settings for the client are read from the @github[reference.conf](/kudu/src/main/resources/reference.conf) file.
A manually initialized client can be injected to the stream using @scaladoc[KuduAttributes](akka.stream.alpakka.kudu.KuduAttributes$)

Scala
:   @@snip [snip](/kudu/src/test/scala/docs/scaladsl/KuduTableSpec.scala) { #attributes }

Java
:   @@snip [snip](/kudu/src/test/java/docs/javadsl/KuduTableTest.java) { #attributes }

## Writing to Kudu in a Flow

Scala
: @@snip [snip](/kudu/src/test/scala/docs/scaladsl/KuduTableSpec.scala) { #flow }

Java
: @@snip [snip](/kudu/src/test/java/docs/javadsl/KuduTableTest.java) { #flow }


## Writing to Kudu with a Sink

Scala
: @@snip [snip](/kudu/src/test/scala/docs/scaladsl/KuduTableSpec.scala) { #sink }

Java
: @@snip [snip](/kudu/src/test/java/docs/javadsl/KuduTableTest.java) { #sink }
