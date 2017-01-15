# RecordIO Framing

The codec parses an incoming RecordIO-formatted stream of ByteStrings into distinct frames.

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
      <artifactId>akka-stream-alpakka-simple-codecs_$scala.binaryVersion$</artifactId>
      <version>$version$</version>
    </dependency>
    ```
    @@@

Gradle
:   @@@vars
    ```gradle
    dependencies {
      compile group: "com.lightbend.akka", name: "akka-stream-alpakka-simple-codecs_$scala.binaryVersion$", version: "$version$"
    }
    ```
    @@@

## Usage

The helper object @scaladoc[RecordIOFraming](akka.stream.alpakka.recordio.scaladsl.RecordIOFraming) provides a `Flow[ByteString, ByteString, _]` which parses out RecordIO frames.

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
