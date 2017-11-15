# File Connectors

The File connectors provides additional connectors for filesystems complementing
the sources and sinks for files already included in core Akka Streams
(which can be found in `akka.stream.javadsl.FileIO` and `akka.stream.scaladsl.FileIO`).

## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-file_$scalaBinaryVersion$
  version=$version$
}

## Usage

### FileTailSource

The `FileTailSource` starts at a given offset in a file and emits chunks of bytes until reaching
the end of the file, it will then poll the file for changes and emit new changes as they are written
 to the file (unless there is backpressure).

A very common use case is combining reading bytes with parsing the bytes into lines, therefore
`FileTailSource` contains a few factory methods to create a source that parses the bytes into
lines and emits those.

In this sample we simply tail the lines of a file and print them to standard out:

Scala
: @@snip ($alpakka$/file/src/test/scala/akka/stream/alpakka/file/scaladsl/FileTailSourceSpec.scala) { #simple-lines }

Java
: @@snip ($alpakka$/file/src/test/java/akka/stream/alpakka/file/javadsl/FileTailSourceTest.java) { #simple-lines }

@scala[@github[Source on Github](/file/src/test/scala/akka/stream/alpakka/file/scaladsl/FileTailSourceSpec.scala) { #simple-lines }]
@java[@github[Source on Github](/file/src/test/java/akka/stream/alpakka/file/javadsl/FileTailSourceTest.java) { #simple-lines }]


### Directory

`Directory.ls(path)` lists all files and directories
directly in a given directory:

Scala
: @@snip ($alpakka$/file/src/test/scala/akka/stream/alpakka/file/scaladsl/DirectorySpec.scala) { #ls }

Java
: @@snip ($alpakka$/file/src/test/java/akka/stream/alpakka/file/javadsl/DirectoryTest.java) { #ls }

`Directory.walk(path)` traverses all subdirectories and lists
files and directories depth first:

Scala
: @@snip ($alpakka$/file/src/test/scala/akka/stream/alpakka/file/scaladsl/DirectorySpec.scala) { #walk }

Java
: @@snip ($alpakka$/file/src/test/java/akka/stream/alpakka/file/javadsl/DirectoryTest.java) { #walk }

@scala[@github[Source on Github](/file/src/test/scala/akka/stream/alpakka/file/scaladsl/DirectorySpec.scala)]
@java[@github[Source on Github](/file/src/test/java/akka/stream/alpakka/file/javadsl/DirectoryTest.java)]


### DirectoryChangesSource

The `DirectoryChangesSource` will emit elements every time there is a change to a watched directory
in the local filesystem, the emitted change concists of the path that was changed and an enumeration
describing what kind of change it was.

In this sample we simply print each change to the directory to standard output:

Scala
: @@snip ($alpakka$/file/src/test/scala/akka/stream/alpakka/file/scaladsl/DirectoryChangesSourceSpec.scala) { #minimal-sample }

Java
: @@snip ($alpakka$/file/src/test/java/akka/stream/alpakka/file/javadsl/DirectoryChangesSourceTest.java) { #minimal-sample }

@scala[@github[Source on Github](/file/src/test/scala/akka/stream/alpakka/file/scaladsl/DirectoryChangesSourceSpec.scala) { #minimal-sample }]
@java[@github[Source on Github](/file/src/test/java/akka/stream/alpakka/file/javadsl/DirectoryChangesSourceTest.java) { #minimal-sample }]


### LogRotationSink

The `LogRotationSink` will create and write to multiple files.  
This sink will takes a function as parameter which returns a
 `Bytestring => Option[Path]` function. If the generated function returns a path
 the sink will rotate the file output to this new path and the actual `ByteString` will be
  written to this new file too.
 With this approach the user can define a custom stateful file generation implementation.

The java implementation is a bit different. The inner function must return null or Path instead of the Option.

A small snippet for the usage:

Scala
: @@snip (../../../../file/src/test/scala/akka/stream/alpakka/file/scaladsl/LogRotatorSinkSpec.scala) { #LogRotationSink-sample }

In this sample we create a size based rotation function:

Scala
: @@snip (../../../../file/src/test/scala/akka/stream/alpakka/file/scaladsl/LogRotatorSinkSpec.scala) { #LogRotationSink-filesize-sample }

Java
: @@snip (../../../../file/src/test/java/akka/stream/alpakka/file/javadsl/LogRotatorSinkTest.java) { #LogRotationSink-filesize-sample }

@scala[@github[Source on Github](/file/src/test/scala/akka/stream/alpakka/file/scaladsl/LogRotatorSinkSpec.scala) { #LogRotationSink-filesize-sample }]
@java[@github[Source on Github](/file/src/test/java/akka/stream/alpakka/file/javadsl/LogRotatorSinkTest.java) { #LogRotationSink-filesize-sample }]


In this sample we create a time based rotation function:

Scala
: @@snip (../../../../file/src/test/scala/akka/stream/alpakka/file/scaladsl/LogRotatorSinkSpec.scala) { #LogRotationSink-timebased-sample }

Java
: @@snip (../../../../file/src/test/java/akka/stream/alpakka/file/javadsl/LogRotatorSinkTest.java) { #LogRotationSink-timebased-sample }

@scala[@github[Source on Github](/file/src/test/scala/akka/stream/alpakka/file/scaladsl/LogRotatorSinkSpec.scala) { #LogRotationSink-timebased-sample }]
@java[@github[Source on Github](/file/src/test/java/akka/stream/alpakka/file/javadsl/LogRotatorSinkTest.java) { #LogRotationSink-timebased-sample }]


### Running the example code

Both the samples are contained in standalone runnable mains, they can be run
 from `sbt` like this:

Scala
:   ```
    sbt
    // tail source
    > akka-stream-alpakka-file/test:runMain akka.stream.alpakka.file.scaladsl.FileTailSourceSpec /some/path/toa/file
    // or directory changes
    > akka-stream-alpakka-file/test:runMain akka.stream.alpakka.file.scaladsl.DirectoryChangesSourceSpec /some/directory/path
    ```

Java
:   ```
    sbt
    // tail source
    > akka-stream-alpakka-file/test:runMain akka.stream.alpakka.file.javadsl.FileTailSourceTest /some/path/toa/file
    // or directory changes
    > akka-stream-alpakka-file/test:runMain akka.stream.alpakka.file.javadsl.DirectoryChangesSourceTest /some/directory/path
    ```
