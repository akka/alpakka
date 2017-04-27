# RecordIO Framing

The codec parses a ByteString stream in the
[RecordIO format](http://mesos.apache.org/documentation/latest/scheduler-http-api/#recordio-response-format) into distinct frames.

For instance, the response body:
```
128\n
{"type": "SUBSCRIBED","subscribed": {"framework_id": {"value":"12220-3440-12532-2345"},"heartbeat_interval_seconds":15.0}20\n
{"type":"HEARTBEAT"}
```
is parsed into frames:
```
{"type": "SUBSCRIBED","subscribed": {"framework_id": {"value":"12220-3440-12532-2345"},"heartbeat_interval_seconds":15.0}
```
```
{"type":"HEARTBEAT"}
```

## Artifacts

sbt
:   @@@vars
    ```scala
    libraryDependencies += "com.lightbend.akka" %% "akka-stream-alpakka-simple-codecs" % "$version$"
    ```
    @@@

Maven
:   @@@vars
    ```xml
    <dependency>
      <groupId>com.lightbend.akka</groupId>
      <artifactId>akka-stream-alpakka-simple-codecs_$scalaBinaryVersion$</artifactId>
      <version>$version$</version>
    </dependency>
    ```
    @@@

Gradle
:   @@@vars
    ```gradle
    dependencies {
      compile group: "com.lightbend.akka", name: "akka-stream-alpakka-simple-codecs_$scalaBinaryVersion$", version: "$version$"
    }
    ```
    @@@

## Usage

The helper object @scaladoc[RecordIOFraming](akka.stream.alpakka.recordio.scaladsl.RecordIOFraming$) provides a `scanner`
factory method for a `Flow[ByteString, ByteString, _]` which parses out RecordIO frames.

For instance, given the sample input:

Scala
: @@snip (../../../../simple-codecs/src/test/scala/akka/stream/alpakka/recordio/RecordIOFramingSpec.scala) { #test-data }

Running it through the RecordIO framing flow:

Scala
: @@snip (../../../../simple-codecs/src/test/scala/akka/stream/alpakka/recordio/RecordIOFramingSpec.scala) { #run-via-scanner }

We obtain:

Scala
: @@snip (../../../../simple-codecs/src/test/scala/akka/stream/alpakka/recordio/RecordIOFramingSpec.scala) { #result }

### Running the example code

The code in this guide is part of runnable tests of this project. You are welcome to edit the code and run it in sbt.

Scala
:   ```
    sbt
    > simpleCodecs/testOnly *.RecordIOFramingSpec
    ```
