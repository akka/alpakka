# Avro Parquet

The Avro Parquet connector provides an Akka Stream Source, Sink and Flow for push and pull data to and from parquet files.

For more information about Apache Parquet please visit the [official documentation](https://parquet.apache.org/documentation/latest/).

@@project-info{ projectId="avroparquet" }

## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-avroparquet_$scala.binary.version$
  version=$project.version$
  symbol2=AkkaVersion
  value2=$akka.version$
  group2=com.typesafe.akka
  artifact2=akka-stream_$scala.binary.version$
  version2=AkkaVersion
}

The table below shows direct dependencies of this module and the second tab shows all libraries it depends on transitively.

@@dependencies { projectId="avroparquet" }

## Source Initiation

Sometimes it might be useful to use parquet file as stream Source. For this we will need to create `AvroParquetReader` 
instance which will produce records as a subtypes of `GenericRecord`, the abstract representation of a record in parquet.
 
Scala
: @@snip (/avroparquet/src/test/scala/docs/scaladsl/AbstractAvroParquet.scala) { #prepare-source #init-reader }

Java
: @@snip (/avroparquet/src/test/java/docs/javadsl/Examples.java) { #init-reader }

After that, you can create the parquet Source from the initialisation of `AvroParquetReader`, this object requires an instance of 
  a `org.apache.parquet.hadoop.ParquetReader` typed by a subtype of `GenericRecord`.

Scala
: @@snip (/avroparquet/src/test/scala/docs/scaladsl/AvroParquetSourceSpec.scala) { #init-source }

Java
: @@snip (/avroparquet/src/test/java/docs/javadsl/Examples.java) { #init-source }

## Sink Initiation

On the other hand, you can use `AvroParquetWriter`, as the akka streams Sink implementation for writing to parquet. 
In that case, its initialisation would require an instance of `org.apache.parquet.hadoop.ParquetWriter`, it will also expect any subtype of `GenericRecord` to be passed.
 
Scala
: @@snip (/avroparquet/src/test/scala/docs/scaladsl/AbstractAvroParquet.scala) { #prepare-sink }

Java
: @@snip (/avroparquet/src/test/java/docs/javadsl/AvroParquetSinkTest.java) { #init-writer }

After that, the AvroParquet Sink can already be used. [Akka HTTP](akka-http:)
In order to demonstrate that *any* subtype of `GenericRecord` can be passed to the stream, the below Scala example 
represents it by passing instances of `com.sksamuel.avro4s.Record`, which it is a type that implements the `GenericRecord` avro interface.
See [Avro4s](https://github.com/sksamuel/avro4s:) or [Avrohugger](https://github.com/julianpeeters/avrohugger:)
) between other ways of generating these classes.
 
Scala
: @@snip (/avroparquet/src/test/scala/docs/scaladsl/AvroParquetSinkSpec.scala) { #init-sink }

Java
: @@snip (/avroparquet/src/test/java/docs/javadsl/AvroParquetSinkTest.java) { #init-sink }

## Flow Initiation

The representation of a ParquetWriter as a Flow is also available to use as a streams flow stage, in which as well as per the other representations, it will expect subtypes of the Parquet `GenericRecord` type to be passed.
 In which as a result, writes into a Parquet file and return the same `GenericRecord`s. Such Flow stage can be easily created by using the `AvroParquetFlow` and providing an `AvroParquetWriter` instance as parameter.

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
