# RecordIO Framing

The codec parses a ByteString stream in the
[RecordIO format](http://mesos.apache.org/documentation/latest/recordio/) used by Apache Mesos into distinct frames.

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


@@project-info{ projectId="simple-codecs" }

## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-simple-codecs_$scala.binary.version$
  version=$project.version$
}

The table below shows direct dependencies of this module and the second tab shows all libraries it depends on transitively.

@@dependencies { projectId="simple-codecs" }


## Usage

The flow factory @apidoc[RecordIOFraming$] provides a `scanner`
factory method for a @scala[`Flow[ByteString, ByteString, _]`]@java[`Flow<ByteString, ByteString, ?>`] which parses out RecordIO frames.

Scala
: @@snip [snip](/simple-codecs/src/test/scala/docs/scaladsl/RecordIOFramingSpec.scala) { #run-via-scanner }

Java
: @@snip [snip](/simple-codecs/src/test/java/docs/javadsl/RecordIOFramingTest.java) { #run-via-scanner }

We obtain:

Scala
: @@snip [snip](/simple-codecs/src/test/scala/docs/scaladsl/RecordIOFramingSpec.scala) { #result }

Java
: @@snip [snip](/simple-codecs/src/test/java/docs/javadsl/RecordIOFramingTest.java) { #result }


### Running the example code

The code in this guide is part of runnable tests of this project. You are welcome to edit the code and run it in sbt.

Scala
:   ```
    sbt
    > simpleCodecs/testOnly *.RecordIOFramingSpec
    ```
