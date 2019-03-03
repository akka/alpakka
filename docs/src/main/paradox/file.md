# File

The File connectors provide additional connectors for filesystems complementing
the sources and sinks for files already included in core Akka Streams
(which can be found in @java[@javadoc[akka.stream.javadsl.FileIO](akka.stream.javadsl.FileIO$)]@scala[@scaladoc[akka.stream.scaladsl.FileIO](akka.stream.scaladsl.FileIO$)]).

@@project-info{ projectId="file" }

## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-file_$scala.binary.version$
  version=$project.version$
}

The table below shows direct dependencies of this module and the second tab shows all libraries it depends on transitively.

@@dependencies { projectId="file" }


## Writing to and reading from files

Use the `FileIO` class to create streams reading from or writing to files. It is part part of Akka streams. 

[Akka Streaming File IO documentation](https://doc.akka.io/docs/akka/current/stream/stream-io.html#streaming-file-io)


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

The @scala[@scaladoc[LogRotatatorSink](akka.stream.alpakka.file.scaladsl.LogRotatorSink$)]
 @java[@scaladoc[LogRotatatorSink](akka.stream.alpakka.file.javadsl.LogRotatorSink$)] will create and 
 write to multiple files.  
This sink takes a creator as parameter which returns a
 @scala[`Bytestring => Option[Path]` function]@java[`Function<ByteString, Optional<Path>>`]. If the generated function returns a path
 the sink will rotate the file output to this new path and the actual `ByteString` will be
  written to this new file too.
 With this approach the user can define a custom stateful file generation implementation.

This example usage shows the built-in target file creation and a custom sink factory which is required to use @scala[@scaladoc[Compression](akka.stream.scaladsl.Compression$)]@java[@scaladoc[Compression](akka.stream.javadsl.Compression$)] for the target files.

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
