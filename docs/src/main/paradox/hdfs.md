# Hadoop Distributed File System - HDFS

The connector offers Flows and Sources that interact with HDFS file systems.

For more information about Hadoop, please visit the [Hadoop documentation](https://hadoop.apache.org/).

### Reported issues

[Tagged issues at Github](https://github.com/akka/alpakka/labels/p%3Ahdfs)

## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-hdfs_$scalaBinaryVersion$
  version=$version$
}

## Specifying a Hadoop Version

By default, HDFS connector uses Hadoop **3.1.0**. If you are using a different version of Hadoop, 
you should exclude the Hadoop libraries from the connector dependency and add the dependency for your preferred version.

## Set up client

Flows provided by this connector need a prepared `org.apache.hadoop.fs.FileSystem` to 
interact with HDFS.


Scala
: @@snip ($alpakka$/hdfs/src/test/scala/akka/stream/alpakka/hdfs/HdfsWriterSpec.scala) { #init-client }

Java
: @@snip ($alpakka$/hdfs/src/test/java/akka/stream/alpakka/hdfs/HdfsWriterTest.java) { #init-client }


## Writing

The connector provides three Flows. Each flow requires `RotationStrategy` and `SyncStrategy` to run.
@scala[@scaladoc[HdfsFlow](akka.stream.alpakka.hdfs.scaladsl.HdfsFlow$).]
@java[@scaladoc[HdfsFlow](akka.stream.alpakka.hdfs.javadsl.HdfsFlow$).]

The flows push `OutgoingMessage` to a downstream.

### Data Writer

Use `HdfsFlow.data` to stream with `FSDataOutputStream` without any compression.


Scala
: @@snip ($alpakka$/hdfs/src/test/scala/akka/stream/alpakka/hdfs/HdfsWriterSpec.scala) { #define-data }

Java
: @@snip ($alpakka$/hdfs/src/test/java/akka/stream/alpakka/hdfs/HdfsWriterTest.java) { #define-data }


### Compressed Data Writer

First, create `CompressionCodec`.


Scala
: @@snip ($alpakka$/hdfs/src/test/scala/akka/stream/alpakka/hdfs/HdfsWriterSpec.scala) { #define-codec }

Java
: @@snip ($alpakka$/hdfs/src/test/java/akka/stream/alpakka/hdfs/HdfsWriterTest.java) { #define-codec }


Then, use `HdfsFlow.compress` to stream with `CompressionOutputStream` and `CompressionCodec`. 


Scala
: @@snip ($alpakka$/hdfs/src/test/scala/akka/stream/alpakka/hdfs/HdfsWriterSpec.scala) { #define-compress }

Java
: @@snip ($alpakka$/hdfs/src/test/java/akka/stream/alpakka/hdfs/HdfsWriterTest.java) { #define-compress }


### Sequence Writer

Use `HdfsFlow.sequence` to stream a flat file consisting of binary key/value pairs.

#### Without Compression


Scala
: @@snip ($alpakka$/hdfs/src/test/scala/akka/stream/alpakka/hdfs/HdfsWriterSpec.scala) { #define-sequence }

Java
: @@snip ($alpakka$/hdfs/src/test/java/akka/stream/alpakka/hdfs/HdfsWriterTest.java) { #define-sequence }


#### With Compression

First, define a codec.


Scala
: @@snip ($alpakka$/hdfs/src/test/scala/akka/stream/alpakka/hdfs/HdfsWriterSpec.scala) { #define-codec }

Java
: @@snip ($alpakka$/hdfs/src/test/java/akka/stream/alpakka/hdfs/HdfsWriterTest.java) { #define-codec }


Then, create a flow.


Scala
: @@snip ($alpakka$/hdfs/src/test/scala/akka/stream/alpakka/hdfs/HdfsWriterSpec.scala) { #define-sequence-compressed }

Java
: @@snip ($alpakka$/hdfs/src/test/java/akka/stream/alpakka/hdfs/HdfsWriterTest.java) { #define-sequence-compressed }

### Passing data through HdfsFlow

Use `HdfsFlow.dataWithPassThrough`, `HdfsFlow.compressedWithPassThrough` or `HdfsFlow.sequenceWithPassThrough`.

When streaming documents from Kafka, you might want to commit to Kafka. The flow will emit two messages.
For every input, it will produce `WrittenMessage` and when it rotates, `RotationMessage`.

Let's say that we have these classes.


Scala
: @@snip ($alpakka$/hdfs/src/test/scala/akka/stream/alpakka/hdfs/HdfsWriterSpec.scala) { #define-kafka-classes }

Java
: @@snip ($alpakka$/hdfs/src/test/java/akka/stream/alpakka/hdfs/HdfsWriterTest.java) { #define-kafka-classes }


Then, we can stream with `passThrough`.


Scala
: @@snip ($alpakka$/hdfs/src/test/scala/akka/stream/alpakka/hdfs/HdfsWriterSpec.scala) { #kafka-example }

Java
: @@snip ($alpakka$/hdfs/src/test/java/akka/stream/alpakka/hdfs/HdfsWriterTest.java) { #kafka-example }


## Configuration

We can configure the sink by `HdfsWritingSettings`. 


Scala
: @@snip ($alpakka$/hdfs/src/test/scala/akka/stream/alpakka/hdfs/HdfsWriterSpec.scala) { #define-settings }

Java
: @@snip ($alpakka$/hdfs/src/test/java/akka/stream/alpakka/hdfs/HdfsWriterTest.java) { #define-settings }


### File path generator

`FilePathGenerator` provides a functionality to generate rotation path in HDFS. 
@scala[@scaladoc[FilePathGenerator](akka.stream.alpakka.hdfs.FilePathGenerator$).]
@java[@scaladoc[FilePathGenerator](akka.stream.alpakka.hdfs.FilePathGenerator$).]

Scala
: @@snip ($alpakka$/hdfs/src/test/scala/akka/stream/alpakka/hdfs/HdfsWriterSpec.scala) { #define-generator }

Java
: @@snip ($alpakka$/hdfs/src/test/java/akka/stream/alpakka/hdfs/HdfsWriterTest.java) { #define-generator }


### Rotation Strategy


`RotationStrategy` provides a functionality to decide when to rotate files.
@scala[@scaladoc[RotationStrategy](akka.stream.alpakka.hdfs.RotationStrategyFactory$).]
@java[@scaladoc[RotationStrategy](akka.stream.alpakka.hdfs.RotationStrategyFactory$).]


### Sync Strategy


`SyncStrategy` provides a functionality to decide when to synchronize the output.
@scala[@scaladoc[SyncStrategy](akka.stream.alpakka.hdfs.SyncStrategyFactory$).]
@java[@scaladoc[SyncStrategy](akka.stream.alpakka.hdfs.SyncStrategyFactory$).]

## Reading

Use `HdfsSource` to read from HDFS.
@scala[@scaladoc[HdfsSource](akka.stream.alpakka.hdfs.scaladsl.HdfsSource$).]
@java[@scaladoc[HdfsSource](akka.stream.alpakka.hdfs.javadsl.HdfsSource$).]


### Data Reader


Scala
: @@snip ($alpakka$/hdfs/src/test/scala/akka/stream/alpakka/hdfs/HdfsReaderSpec.scala) { #define-data-source }

Java
: @@snip ($alpakka$/hdfs/src/test/java/akka/stream/alpakka/hdfs/HdfsReaderTest.java) { #define-data-source }


### Compressed Data Reader


Scala
: @@snip ($alpakka$/hdfs/src/test/scala/akka/stream/alpakka/hdfs/HdfsReaderSpec.scala) { #define-compressed-source }

Java
: @@snip ($alpakka$/hdfs/src/test/java/akka/stream/alpakka/hdfs/HdfsReaderTest.java) { #define-compressed-source }


### Sequence Reader


Scala
: @@snip ($alpakka$/hdfs/src/test/scala/akka/stream/alpakka/hdfs/HdfsReaderSpec.scala) { #define-sequence-source }

Java
: @@snip ($alpakka$/hdfs/src/test/java/akka/stream/alpakka/hdfs/HdfsReaderTest.java) { #define-sequence-source }


## Running the example code

The code in this guide is part of runnable tests of this project. You are welcome to edit the code and run it in sbt.

Scala
:   ```
    sbt
    > hdfs/testOnly *.HdfsWriterSpec
    > hdfs/testOnly *.HdfsReaderSpec
    ```

Java
:   ```
    sbt
    > hdfs/testOnly *.HdfsWriterTest
    > hdfs/testOnly *.HdfsReaderTest
    ```
