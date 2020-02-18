# Avro Parquet

The Avro Parquet connector provides an Akka Stream Source, Sink and Flow for push and pull data to and from parquet files.

For more information about Apache Parquet please visit the [official documentation](https://parquet.apache.org/documentation/latest/).

@@project-info{ projectId="avroparquet" }

## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-avroparquet_$scala.binary.version$
  version=$project.version$
}

The table below shows direct dependencies of this module and the second tab shows all libraries it depends on transitively.

@@dependencies { projectId="avroparquet" }

## Source Initiation

We will need an @apidoc[akka.actor.ActorSystem] and an @apidoc[akka.stream.Materializer].

Scala
: @@snip (/avroparquet/src/test/scala/docs/scaladsl/AbstractAvroParquet.scala) { #init-system }

Java
: @@snip (/avroparquet/src/test/java/docs/javadsl/Examples.java) { #init-system }

Sometimes it might be useful to use parquet file as stream Source. For this we will need to create `AvroParquetReader` 
instance which produces Parquet `GenericRecord` instances.
 
Scala
: @@snip (/avroparquet/src/test/scala/docs/scaladsl/AvroParquetSourceSpec.scala) { #init-reader } 

Java
: @@snip (/avroparquet/src/test/java/docs/javadsl/Examples.java) { #init-reader }

After it, you can create your Source object which accepts instance of `AvroParquetReader` as parameter 

Scala
: @@snip (/avroparquet/src/test/scala/docs/scaladsl/AvroParquetSourceSpec.scala) { #init-source }

Java
: @@snip (/avroparquet/src/test/java/docs/javadsl/Examples.java) { #init-source }

## Sink Initiation

Sometimes it might be useful to use Parquet file as akka stream Sink. For an instance, if you need to store data on 
Parquet files on HDFS (or any other distributed file system) and perform map-reduce jobs on it further. 
For this we first of all need to create `AvroParquetWriter` instance which accepts `GenericRecord`.
 
Scala
: @@snip (/avroparquet/src/test/scala/docs/scaladsl//AvroParquetSinkSpec.scala) { #init-writer } 

Java
: @@snip (/avroparquet/src/test/java/docs/javadsl/AvroParquetSinkTest.java) { #init-writer }

After it, you can create Sink which accepts instance of `AvroParquetWriter` as parameter. 
 
Scala
: @@snip (/avroparquet/src/test/scala/docs/scaladsl//AvroParquetSinkSpec.scala) { #init-sink }

Java
: @@snip (/avroparquet/src/test/java/docs/javadsl/AvroParquetSinkTest.java) { #init-sink }

## Flow Initiation

It might be useful to use ParquetWriter as the streams flow stage, which accepts Parquet `GenericRecord`, writes it to
Parquet file, and returns the same `GenericRecords`. Such Flow stage can be easily created by creating `AvroParquetFlow`
instance and providing `AvroParquetWriter` instance as parameter.

Scala
: @@snip (/avroparquet/src/test/scala/docs/scaladsl/AvroParquetFlowSpec.scala) { #init-flow }
This is all preparation that we are going to need.

Java
: @@snip (/avroparquet/src/test/java/docs/javadsl/Examples.java) { #init-flow }

## Running the example code

The code in this guide is part of runnable tests of this project. You are welcome to edit the code and run it in sbt.

Scala
:   ```
    sbt
    > avroparquet/test
    ```
