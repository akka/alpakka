# IBM EventStore

IBM EventStore is an in-memory database built on top of Apache Spark and Apache Parquet Data Format, the Alpakka IBM EventStore connector provides an Akka Stream flow and sink for insertion of records into a table.
 
## External references 

[IBM EventStore Documentation](https://www.ibm.com/support/knowledgecenter/SSGNPV/eventstore/desktop/welcome.html)

## Artifacts

sbt
:   @@@vars
    ```scala
    libraryDependencies += "com.lightbend.akka" %% "akka-stream-alpakka-ibm-eventstore" % "$version$"
    ```
    @@@

Maven
:   @@@vars
    ```xml
    <dependency>
      <groupId>com.lightbend.akka</groupId>
      <artifactId>akka-stream-alpakka-ibm-eventstore_$scalaBinaryVersion$</artifactId>
      <version>$version$</version>
    </dependency>
    ```
    @@@

Gradle
:   @@@vars
    ```gradle
    dependencies {
      compile group: "com.lightbend.akka", name: "akka-stream-alpakka-ibm-eventstore_$scalaBinaryVersion$", version: "$version$"
    }
    ```
    @@@

## Usage

To use the EventStore sink and flow you first need to configure the EventStore client. When done inserting records into EventStore, you need to terminate the EventStore client using cleanUp. 
 
`ConfigurationReader.setConnectionEndpoints` - Is used to connect to the EventStore cluster

`EventContext.cleanUp` - Is used to terminate a connection, this has to be called to successfully shutdown the application.

These functions are provided by the EventStore client library as a transient dependencies.

### Set EventStore connection endpoints

Scala

: @@snip (../../../../ibm-eventstore/src/test/scala/akka/stream/alpakka/ibm/eventstore/scaladsl/EventStoreSpec.scala) { #configure-endpoint }

Java
: @@snip (../../../../ibm-eventstore/src/test/java/akka/stream/alpakka/ibm/eventstore/javadsl/EventStoreSpec.java) { #configure-endpoint }

### Insert rows into a table

Scala

: @@snip (../../../../ibm-eventstore/src/test/scala/akka/stream/alpakka/ibm/eventstore/scaladsl/EventStoreSpec.scala) { #insert-rows }

Java
: @@snip (../../../../ibm-eventstore/src/test/java/akka/stream/alpakka/ibm/eventstore/javadsl/EventStoreSpec.java) { #insert-rows }

### Insert rows into a table using a flow

Scala

: @@snip (../../../../ibm-eventstore/src/test/scala/akka/stream/alpakka/ibm/eventstore/scaladsl/EventStoreSpec.scala) { #insert-rows-using-flow }

Java
: @@snip (../../../../ibm-eventstore/src/test/java/akka/stream/alpakka/ibm/eventstore/javadsl/EventStoreSpec.java) { #insert-rows-using-flow }

### Shutdown and clean up of the EventStore client

Scala

: @@snip (../../../../ibm-eventstore/src/test/scala/akka/stream/alpakka/ibm/eventstore/scaladsl/EventStoreSpec.scala) { #cleanup }

Java
: @@snip (../../../../ibm-eventstore/src/test/java/akka/stream/alpakka/ibm/eventstore/javadsl/EventStoreSpec.java) { #cleanup }
