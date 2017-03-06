# Comma Separated Files - CSV

Blah blah.

## Artifacts

sbt
:   @@@vars
    ```scala
    libraryDependencies += "com.lightbend.akka" %% "akka-stream-alpakka-csv" % "$version$"
    ```
    @@@

Maven
:   @@@vars
    ```xml
    <dependency>
      <groupId>com.lightbend.akka</groupId>
      <artifactId>akka-stream-alpakka-csv_$scala.binaryVersion$</artifactId>
      <version>$version$</version>
    </dependency>
    ```
    @@@

Gradle
:   @@@vars
    ```gradle
    dependencies {
      compile group: "com.lightbend.akka", name: "akka-stream-alpakka-csv_$scala.binaryVersion$", version: "$version$"
    }
    ```
    @@@

## Usage

### CSV parsing

Blah blah.

In this sample we ...:

Scala
: @@snip (../../../../file/src/test/scala/akka/stream/alpakka/file/scaladsl/FileTailSourceSpec.scala) { #simple-lines }

Java
: @@snip (../../../../file/src/test/java/akka/stream/alpakka/file/javadsl/FileTailSourceTest.java) { #simple-lines }


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
