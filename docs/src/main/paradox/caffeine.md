# Caffeine Flow

The [Caffeine](https://github.com/ben-manes/caffeine) flow provides Akka Stream flow, to reconcile data in memory.


Let imagine 3 event types with a correlation id:
 
scala
:   @@snip (../../../../caffeine/src/test/scala/akka/stream/alpakka/caffeine/scaladsl/CaffeineFlowSpec.scala) { #events }

And an aggregate class:

scala
:   @@snip (../../../../caffeine/src/test/scala/akka/stream/alpakka/caffeine/scaladsl/CaffeineFlowSpec.scala) { #aggregated-event }


This 3 different events type can enter in the flow. This Caffeine Flow will try to gather those events on their correlation id.

An aggregation class will be emitted:

* as completed, if all event for a given id entered the flow during an interval of time.
* as expired, after the timeouts.

 
## Artifacts

sbt
:   @@@vars
    ```scala
    libraryDependencies += "com.typesafe.akka" %% "akka-stream-alpakka-caffeine" % "$version$"
    ```
    @@@

Maven
:   @@@vars
    ```xml
    <dependency>
      <groupId>com.typesafe.akka</groupId>
      <artifactId>akka-stream-alpakka-caffeine_$scala.binaryVersion$</artifactId>
      <version>$version$</version>
    </dependency>
    ```
    @@@

Gradle
:   @@@vars
    ```gradle
    dependencies {
      compile group: "com.typesafe.akka", name: "akka-stream-alpakka-caffeine_$scala.binaryVersion$", version: "$version$"
    }
    ```
    @@@

## Usage

Reconcilation logic must be provided under the form of an Aggregator.


scala
:   @@snip (../../../../caffeine/src/test/scala/akka/stream/alpakka/caffeine/scaladsl/CaffeineFlowSpec.scala) { #aggregator }



2 flow stages are provided

### CaffeineFanOut2Stage

2 outputs that emit reconciled and expired events.

### CaffeineFlowStage

1 OutLet that emit an Either

* Right containing reconciled events.
* Left containing expired events
