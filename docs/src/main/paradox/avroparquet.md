#Avro Parquet

The Avro Parquet connector provides an Akka Stream Source and Sink for push and pull data to and from parquet files.

For more information about Apache Parquet please visit the [official documentation](https://parquet.apache.org/documentation/latest/).

## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-avroparquet_$scalaBinaryVersion$
  version=$version$
}

#Usage

We will need an @scaladoc[ActorSystem](akka.actor.ActorSystem) and an @scaladoc[ActorMaterializer](akka.stream.ActorMaterializer).

Scala
: @@snip (/avroparquet/src/test/scala/akka/stream/alpakka/avroparquet/scaladsl/AbstractAvroParquet.scala) { #init-system }

Java
: @@snip (/doc-examples/src/main/java/avroparquet/Examples.java) { #init-system }

#Sink Initiation

Scala
: @@snip (/avroparquet/src/test/scala/akka/stream/alpakka/avroparquet/scaladsl/AvroParquetSinkSpec.scala) { #init-sink }

Java
: @@snip (/avroparquet/src/test/java/akka/stream/alpakka/avroparquet/javadsl/AvroParquetSinkTest.java) { #init-sink }

#Source Initalization
Scala
: @@snip (/avroparquet/src/test/scala/akka/stream/alpakka/avroparquet/scaladsl/AvroParquetSourceSpec.scala) { #init-source }

Java
: @@snip (/doc-examples/src/main/java/avroparquet/Examples.java) { #init-source }


This is all preparation that we are going to need.

### Running the example code

The code in this guide is part of runnable tests of this project. You are welcome to edit the code and run it in sbt.

Scala
:   ```
    sbt
    > avroparquet/test
    ```