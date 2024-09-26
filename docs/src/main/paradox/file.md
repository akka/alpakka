# File

The File connectors provide additional connectors for filesystems complementing
the sources and sinks for files already included in core Akka Streams
(which can be found in @aipdoc[FileIO$]).

@@project-info{ projectId="file" }

## Artifacts

The Akka dependencies are available from Akka's library repository. To access them there, you need to configure the URL for this repository.

@@repository [sbt,Maven,Gradle] {
id="akka-repository"
name="Akka library repository"
url="https://repo.akka.io/maven"
}

Additionally, add the dependencies as below.

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-file_$scala.binary.version$
  version=$project.version$
  symbol2=AkkaVersion
  value2=$akka.version$
  group2=com.typesafe.akka
  artifact2=akka-stream_$scala.binary.version$
  version2=AkkaVersion
}

The table below shows direct dependencies of this module and the second tab shows all libraries it depends on transitively.

@@dependencies { projectId="file" }


## Writing to and reading from files

Use the `FileIO` class to create streams reading from or writing to files. It is part part of Akka streams. 

[Akka Streaming File IO documentation](https://doc.akka.io/libraries/akka-core/current/stream/stream-io.html#streaming-file-io)


## Tailing a file into a stream

The `FileTailSource` starts at a given offset in a file and emits chunks of bytes until reaching
the end of the file, it will then poll the file for changes and emit new changes as they are written
 to the file (unless there is backpressure).
 
A very common use case is combining reading bytes with parsing the bytes into lines, therefore
`FileTailSource` contains a few factory methods to create a source that parses the bytes into
lines and emits those.

In this sample we simply tail the lines of a file and print them to standard out:

Scala
: @@snip [snip](/file/src/test/scala/docs/scaladsl/FileTailSourceSpec.scala) { #simple-lines }

Java
: @@snip [snip](/file/src/test/java/docs/javadsl/FileTailSourceTest.java) { #simple-lines }

### Shutdown stream when file is deleted

The `FileTailSource` stream will not shutdown or throw an error when the file it is tailing is deleted from the filesystem. 
If you would like to shutdown the stream, or throw an error, you can do so by merging in a @apidoc[(javadsl|scaladsl).DirectoryChangesSource$] that listens to filesystem events in the directory that contains the file.

In the following example, a `DirectoryChangesSource` is used to watch for events in a directory. 
If a file delete event is observed for the file we are tailing then we shutdown the stream gracefully by using a @apidoc[Flow.recoverWithRetries](Flow$) to switch to a @apidoc[Source.empty](Source$), which with immediately send an `OnComplete` signal and shutdown the stream.

Scala
: @@snip [snip](/file/src/test/scala/docs/scaladsl/FileTailSourceExtrasSpec.scala) { #shutdown-on-delete }

Java
: @@snip [snip](/file/src/test/java/docs/javadsl/FileTailSourceTest.java) { #shutdown-on-delete }

@@@ note { title="Stream Shutdown Race Condition" }

Since the `DirectoryChangesSource` and the `FileTailSource` operate asynchronously as separate sources there is the possibility that the stream could be shutdown prematurely.
If the file is detected as deleted and the stream is shutdown before the last element is emitted from `FileTailSource`, then that data will never be available to downstream user stages.

@@@

### Shutdown stream after an idle timeout

It may be useful to shutdown the stream when no new data has been added for awhile to a file being tailed by `FileTailSource`.
In the following example, a @apidoc[Flow.idleTimeout](Flow$) operator is used to trigger a `TimeoutException` that can be recovered with @apidoc[Flow.recoverWithRetries](Flow$) and a @apidoc[Source.empty](Source$) to successfully shutdown the stream.

Scala
: @@snip [snip](/file/src/test/scala/docs/scaladsl/FileTailSourceExtrasSpec.scala) { #shutdown-on-idle-timeout }

Java
: @@snip [snip](/file/src/test/java/docs/javadsl/FileTailSourceTest.java) { #shutdown-on-idle-timeout }


## Creating directories

`Directory.mkdirs()` and `Directory.mkdirsWithContext()` create directories for @javadoc[Path](java.nio.file.Path) elements in the stream. The `withContext`-variant allows the user to pass through additional information with every path.

Scala
: @@snip [snip](/file/src/test/scala/docs/scaladsl/DirectorySpec.scala) { #mkdirs }

Java
: @@snip [snip](/file/src/test/java/docs/javadsl/DirectoryTest.java) { #mkdirs }



## Listing directory contents

`Directory.ls(path)` lists all files and directories
directly in a given directory:

Scala
: @@snip [snip](/file/src/test/scala/docs/scaladsl/DirectorySpec.scala) { #ls }

Java
: @@snip [snip](/file/src/test/java/docs/javadsl/DirectoryTest.java) { #ls }

`Directory.walk(path)` traverses all subdirectories and lists
files and directories depth first:

Scala
: @@snip [snip](/file/src/test/scala/docs/scaladsl/DirectorySpec.scala) { #walk }

Java
: @@snip [snip](/file/src/test/java/docs/javadsl/DirectoryTest.java) { #walk }

## Listening to changes in a directory

The `DirectoryChangesSource` will emit elements every time there is a change to a watched directory
in the local filesystem, the emitted change concists of the path that was changed and an enumeration
describing what kind of change it was.

In this sample we simply print each change to the directory to standard output:

Scala
: @@snip [snip](/file/src/test/scala/docs/scaladsl/DirectoryChangesSourceSpec.scala) { #minimal-sample }

Java
: @@snip [snip](/file/src/test/java/docs/javadsl/DirectoryChangesSourceTest.java) { #minimal-sample }

## Rotating the file to stream into 

The @apidoc[LogRotatorSink$] will create and 
 write to multiple files.  
This sink takes a creator as parameter which returns a
 @scala[`Bytestring => Option[Path]` function]@java[`Function<ByteString, Optional<Path>>`]. If the generated function returns a path
 the sink will rotate the file output to this new path and the actual `ByteString` will be
  written to this new file too.
 With this approach the user can define a custom stateful file generation implementation.

This example usage shows the built-in target file creation and a custom sink factory which is required to use @apidoc[Compression$] for the target files.

Scala
: @@snip [snip](/file/src/test/scala/docs/scaladsl/LogRotatorSinkSpec.scala) { #sample }

Java
: @@snip [snip](/file/src/test/java/docs/javadsl/LogRotatorSinkTest.java) { #sample }

### Example: size-based rotation

Scala
: @@snip [snip](/file/src/test/scala/docs/scaladsl/LogRotatorSinkSpec.scala) { #size }

Java
: @@snip [snip](/file/src/test/java/docs/javadsl/LogRotatorSinkTest.java) { #size }

### Example: time-based rotation

Scala
: @@snip [snip](/file/src/test/scala/docs/scaladsl/LogRotatorSinkSpec.scala) { #time }

Java
: @@snip [snip](/file/src/test/java/docs/javadsl/LogRotatorSinkTest.java) { #time }

### Example: content-based rotation with compression to SFTP file

This example can be found in the @ref:[self-contained example documentation section](examples/ftp-samples.md#example-rotate-data-stream-over-to-multiple-compressed-files-on-sftp-server).

## ZIP Archive

### Writing ZIP Archives

The @apidoc[Archive$]
contains flow for compressing multiple files into one ZIP file.

Result of flow can be send to sink even before whole ZIP file is created, so size of resulting ZIP archive
is not limited to memory size.  

This example usage shows compressing files from disk. 

Scala
: @@snip [snip](/file/src/test/scala/docs/scaladsl/ArchiveSpec.scala) { #sample-zip }

Java
: @@snip [snip](/file/src/test/java/docs/javadsl/ArchiveTest.java) { #sample-zip }

### Reading ZIP archives


@apidoc[Archive.zipReader()](Archive$) reads a file in ZIP format, and emitting the metadata entry and a `Source` for every file in the stream.
It is not needed to emit every file, also multiple files can be emitted in parallel. (Every sub-source will seek into the archive.)

The example below reads the incoming file, and unzip all to the local file system.

Scala
: @@snip [snip](/file/src/test/scala/docs/scaladsl/ArchiveSpec.scala) { #zip-reader }

Java
: @@snip [snip](/file/src/test/java/docs/javadsl/ArchiveTest.java) { #sample-zip-read }


## TAR Archive

### Writing TAR archives

@apidoc[Archive.tar()](Archive$) creates a flow for packaging multiple files into one TAR archive.

Result of flow can be send to sink even before whole TAR file is created, so size of resulting TAR archive
is not limited to memory size.

This example usage shows packaging directories and files from disk.

Scala
: @@snip [snip](/file/src/test/scala/docs/scaladsl/TarArchiveSpec.scala) { #sample-tar }

Java
: @@snip [snip](/file/src/test/java/docs/javadsl/ArchiveTest.java) { #sample-tar }

To produce a gzipped TAR file see the following example.

Scala
: @@snip [snip](/file/src/test/scala/docs/scaladsl/TarArchiveSpec.scala) { #sample-tar-gz }

Java
: @@snip [snip](/file/src/test/java/docs/javadsl/ArchiveTest.java) { #sample-tar-gz }


### Reading TAR archives

@apidoc[Archive.tarReader()](Archive$) reads a stream of `ByteString`s as TAR format emitting the metadata entry and a `Source` for every file in the stream. It is essential to request all the emitted source's data, otherwise the stream will not reach the next file entry.

The example below reads the incoming stream, creates directories and stores all files in the local file system.

Scala
: @@snip [snip](/file/src/test/scala/docs/scaladsl/TarArchiveSpec.scala) { #tar-reader }

Java
: @@snip [snip](/file/src/test/java/docs/javadsl/ArchiveTest.java) { #tar-reader }

The test in @extref[`NestedTarRaderTest`](github:file/src/test/java/docs/javadsl/NestedTarReaderTest.java) illustrates how the tar reader may be used to extract tar archives from within a tar archive.
