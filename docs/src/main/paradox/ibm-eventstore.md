# IBM EventStore

The IBM EventStore connector provides an Akka Stream sink for insertion of records into a table.

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

### Configuration 

The EventStore connector can be configured within your `application.conf` file.

Configuration
: @@snip (../../../../ibm-eventstore/src/main/resources/reference.conf)

### Create EventStore Configuration

Scala

: @@snip (../../../../ibm-eventstore/src/test/scala/akka/stream/alpakka/ibm/eventstore/scaladsl/EventStoreSpec.scala) { #configuration }

Java
: @@snip (../../../../ibm-eventstore/src/test/java/akka/stream/alpakka/ibm/eventstore/javadsl/EventStoreSpec.java) { #configuration }

### Set EventStore connection endpoints

Scala

: @@snip (../../../../ibm-eventstore/src/test/scala/akka/stream/alpakka/ibm/eventstore/scaladsl/EventStoreSpec.scala) { #configure-endpoint }

Java
: @@snip (../../../../ibm-eventstore/src/test/java/akka/stream/alpakka/ibm/eventstore/javadsl/EventStoreSpec.java) { #configure-endpoint }

### Insert rows in table

Scala

: @@snip (../../../../ibm-eventstore/src/test/scala/akka/stream/alpakka/ibm/eventstore/scaladsl/EventStoreSpec.scala) { #insert-rows }

Java
: @@snip (../../../../ibm-eventstore/src/test/java/akka/stream/alpakka/ibm/eventstore/javadsl/EventStoreSpec.java) { #insert-rows }

### Shutdown EventStore client

Scala

: @@snip (../../../../ibm-eventstore/src/test/scala/akka/stream/alpakka/ibm/eventstore/scaladsl/EventStoreSpec.scala) { #cleanup }

Java
: @@snip (../../../../ibm-eventstore/src/test/java/akka/stream/alpakka/ibm/eventstore/javadsl/EventStoreSpec.java) { #cleanup }

## External references 
 
[IBM EventStore Documentation](https://www.ibm.com/support/knowledgecenter/SSGNPV/eventstore/desktop/welcome.html)
