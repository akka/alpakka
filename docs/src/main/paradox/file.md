# Files

The File connectors provide additional connectors for filesystems complementing
the sources and sinks for files already included in core Akka Streams
(which can be found in @java[@javadoc[akka.stream.javadsl.FileIO](akka.stream.javadsl.FileIO$)]@scala[@scaladoc[akka.stream.scaladsl.FileIO](akka.stream.scaladsl.FileIO$)]).

### Reported issues

[Tagged issues at Github](https://github.com/akka/alpakka/labels/p%3Afile)

## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-file_$scalaBinaryVersion$
  version=$version$
}

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
: @@snip [snip](/file/src/test/scala/akka/stream/alpakka/file/scaladsl/FileTailSourceSpec.scala) { #simple-lines }

Java
: @@snip [snip](/file/src/test/java/akka/stream/alpakka/file/javadsl/FileTailSourceTest.java) { #simple-lines }

## Listing directory contents

`Directory.ls(path)` lists all files and directories
directly in a given directory:

Scala
: @@snip [snip](/file/src/test/scala/akka/stream/alpakka/file/scaladsl/DirectorySpec.scala) { #ls }

Java
: @@snip [snip](/file/src/test/java/akka/stream/alpakka/file/javadsl/DirectoryTest.java) { #ls }

`Directory.walk(path)` traverses all subdirectories and lists
files and directories depth first:

Scala
: @@snip [snip](/file/src/test/scala/akka/stream/alpakka/file/scaladsl/DirectorySpec.scala) { #walk }

Java
: @@snip [snip](/file/src/test/java/akka/stream/alpakka/file/javadsl/DirectoryTest.java) { #walk }

## Listening to changes in a directory

The `DirectoryChangesSource` will emit elements every time there is a change to a watched directory
in the local filesystem, the emitted change concists of the path that was changed and an enumeration
describing what kind of change it was.

In this sample we simply print each change to the directory to standard output:

Scala
: @@snip [snip](/file/src/test/scala/akka/stream/alpakka/file/scaladsl/DirectoryChangesSourceSpec.scala) { #minimal-sample }

Java
: @@snip [snip](/file/src/test/java/akka/stream/alpakka/file/javadsl/DirectoryChangesSourceTest.java) { #minimal-sample }

## Rotating the file to stream into 

The @scala[@scaladoc[LogRotatatorSink](akka.stream.alpakka.file.scaladsl.LogRotatorSink$)]
 @java[@scaladoc[LogRotatatorSink](akka.stream.alpakka.file.javadsl.LogRotatorSink$)] will create and 
 write to multiple files.  
This sink will takes a function as parameter which returns a
 @scala[`Bytestring => Option[Path]` function]@java[`Function<ByteString, Optional<Path>>`]. If the generated function returns a path
 the sink will rotate the file output to this new path and the actual `ByteString` will be
  written to this new file too.
 With this approach the user can define a custom stateful file generation implementation.

A small snippet for the usage

Scala
: @@snip [snip](/file/src/test/scala/akka/stream/alpakka/file/scaladsl/LogRotatorSinkSpec.scala) { #sample }

Java
: @@snip [snip](/file/src/test/java/akka/stream/alpakka/file/javadsl/LogRotatorSinkTest.java) { #sample }

### Example: size-based rotation

Scala
: @@snip [snip](/file/src/test/scala/akka/stream/alpakka/file/scaladsl/LogRotatorSinkSpec.scala) { #size }

Java
: @@snip [snip](/file/src/test/java/akka/stream/alpakka/file/javadsl/LogRotatorSinkTest.java) { #size }

### Example: time-based rotation

Scala
: @@snip [snip](/file/src/test/scala/akka/stream/alpakka/file/scaladsl/LogRotatorSinkSpec.scala) { #time }

Java
: @@snip [snip](/file/src/test/java/akka/stream/alpakka/file/javadsl/LogRotatorSinkTest.java) { #time }

### Running the example code

Both the samples are contained in standalone runnable mains, they can be run
 from `sbt` like this:

Scala
:   ```
    sbt
    // tail source
    > file/Test/runMain akka.stream.alpakka.file.scaladsl.FileTailSourceSpec /some/path/toa/file
    // or directory changes
    > file/Test/runMain akka.stream.alpakka.file.scaladsl.DirectoryChangesSourceSpec /some/directory/path
    // File rotator
    > file/Test/runMain akka.stream.alpakka.file.scaladsl.LogRotatorSinkTest
    ```

Java
:   ```
    sbt
    // tail source
    > file/Test/runMain akka.stream.alpakka.file.javadsl.FileTailSourceTest /some/path/toa/file
    // or directory changes
    > file/Test/runMain akka.stream.alpakka.file.javadsl.DirectoryChangesSourceTest /some/directory/path
    // File rotator
    > file/Test/runMain akka.stream.alpakka.file.javadsl.LogRotatorSinkTest
    ```
