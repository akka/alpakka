# Neo4j Connector

[Neo4j](https://neo4j.com/) is an open source, native graph database which allows you to efficiently store, manage and query highly connected
data.

It is an efficient, transactional OLTP database engine, which also allows for graph analytics. 
Its [Cypher query language](https://neo4j.com/developer/cypher/) expresses the graph pattern you're interested in in a very readable ascii-art. 
As the underlying property graph model is very versatile, Neo4j is used across many industries and use-cases, like
recommendations, network management, software analytics, logistics and more.

## Artifacts

sbt
:   @@@vars
    ```scala
    libraryDependencies += "com.lightbend.akka" %% "akka-stream-alpakka-neo4j" % "$version$"
    ```
    @@@

Maven
:   @@@vars
    ```xml
    <dependency>
      <groupId>com.lightbend.akka</groupId>
      <artifactId>akka-stream-alpakka-neo4j_$scala.binaryVersion$</artifactId>
      <version>$version$</version>
    </dependency>
    ```
    @@@

Gradle
:   @@@vars
    ```gradle
    dependencies {
      compile group: "com.lightbend.akka", name: "akka-stream-alpakka-neo4j_$scala.binaryVersion$", version: "$version$"
    }
    ```
    @@@ 
 

## Connect to neo4j

scala
:   @@snip (../../../../neo4j/src/test/scala/akka/stream/alpakka/neo4j/scaladsl/Neo4jBaseSpec.scala) { #connect }

java
:   @@snip (../../../../neo4j/src/test/java/akka/stream/alpakka/neo4j/javadsl/Neo4jBaseTestCase.java) { #connect }


## Marshalling

Marshaller must be provided when reading or writing to a neo4j.

This project provides a generic solution for scala user based on [shapeless](https://github.com/milessabin/shapeless), then case class serializer if not provided will be generated compile time.

Generic Marshaller
:   @@snip (../../../../neo4j/src/main/scala/akka/stream/alpakka/neo4j/scaladsl/ShapelessCypherMarshaller.scala) { #marshaller }

Generic Unmarshaller
:   @@snip (../../../../neo4j/src/main/scala/akka/stream/alpakka/neo4j/scaladsl/ShapelessCypherUnmarshaller.scala) { #unmarshaller }

Java users will have to provides hand crafted marshallers (see below)

## Components

### Finite source

Finite source runs a [cypher](https://neo4j.com/developer/cypher/) query and produces result in a Source.

scala shapeless
:   @@snip (../../../../neo4j/src/test/scala/akka/stream/alpakka/neo4j/scaladsl/Neo4jFiniteSourceSpec.scala) { #source-shapeless }

scala custom
:   @@snip (../../../../neo4j/src/test/scala/akka/stream/alpakka/neo4j/scaladsl/Neo4jFiniteSourceSpec.scala) { #source }

java custom
:   @@snip (../../../../neo4j/src/test/java/akka/stream/alpakka/neo4j/javadsl/Neo4jFiniteSourceTestCase.java) { #source }

### Flow

Flow allow to write in neo4j.

scala shapeless
:   @@snip (../../../../neo4j/src/test/scala/akka/stream/alpakka/neo4j/scaladsl/Neo4jFlowSpec.scala) { #flow-shapeless }

scala custom
:   @@snip (../../../../neo4j/src/test/scala/akka/stream/alpakka/neo4j/scaladsl/Neo4jFlowSpec.scala) { #flow }

java custom
:   @@snip (../../../../neo4j/src/test/java/akka/stream/alpakka/neo4j/javadsl/Neo4jFlowTestCase.java) { #flow }


### Sink


