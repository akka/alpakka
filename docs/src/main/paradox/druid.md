# Druid connector

## Artifacts

sbt
:   @@@vars
    ```scala
    libraryDependencies += "com.lightbend.akka" %% "akka-stream-alpakka-druid" % "$version$"
    ```
    @@@

Maven
:   @@@vars
    ```xml
    <dependency>
      <groupId>com.lightbend.akka</groupId>
      <artifactId>akka-stream-alpakka-druid_$scala.binaryVersion$</artifactId>
      <version>$version$</version>
    </dependency>
    ```
    @@@

Gradle
:   @@@vars
    ```gradle
    dependencies {
      compile group: "com.lightbend.akka", name: "akka-stream-alpakka-druid_$scala.binaryVersion$", version: "$version$"
    }
    ```
    @@@

Sink to publish event in [druid](http://druid.io).

Druid is an OLAP data cube with streaming ingestion support.

It allow sub second query, even on really huge data. 

## Usage

Druid event are sent with Tranquilizer library

Scala
: @@snip (../../../../druid/src/test/scala/akka/stream/alpakka/druid/DruidITTestSuite.scala) { #tranquilizer-settings }

Then build a sink with this config:

Scala
: @@snip (../../../../druid/src/test/scala/akka/stream/alpakka/druid/DruidITTestSuite.scala) { #druid-sink }



## Druid 

Druid is a big beast, production configuration is about 5 nodes ... Hopefully there is a quick start configuration with [imply.io](https://imply.io/).

Download & follow instruction from [imply.io](https://imply.io/), it will start a full druid stack for testing.


```bash
cd path/to/imply
bin/supervise -c conf/supervise/quickstart.conf
```

To activate IT test that relies on druid stack:

```bash
export DRUID_ZOOKEEPER_TEST="localhost:2181"
```

Before running sbt.

IT test will push events in druid, you will see the result in [pivot ui](http://localhost:9095/).



