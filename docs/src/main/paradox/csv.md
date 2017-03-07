# Comma-Separated Values - CSV

Comma-Separated Values are used as interchange format for tabular data 
of text. This format is supported by most 

Despite the name the values are often separated by a semicolon `;`.

Even though the format is interpreted differently there exists a formal specification in [RFC4180](https://tools.ietf.org/html/rfc4180).

The format uses three different characters to structure the data:

* Field Delimiter - separates the columns from each other (e.g. `,` or `;`)
* Quote - marks columns that may contain other structuring characters (such as Field Delimiters or line break) (e.g. `"`)
* Escape Character - used to escape Field Delimiters in columns (e.g. `\`)

Lines are separated by either Line Feed (`\n` = ASCII 10) or Carriage Return and Line Feed (`\r` = ASCII 13 + `\n` = ASCII 10).

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

### CSV parsing and framing 

CSV framing offers a flow that takes a stream of `akka.util.ByteString` and issues a stream of lists of `ByteString`.

Scala
: @@snip (../../../../csv/src/test/scala/akka/stream/alpakka/csv/scaladsl/CsvFramingSpec.scala) { #flow-type }

Java
: @@snip (../../../../csv/src/test/java/akka/stream/alpakka/csv/javadsl/CsvFramingTest.java) { #flow-type }

In this sample we read a single line of CSV formatted data into a list of column elements:

Scala
: @@snip (../../../../csv/src/test/scala/akka/stream/alpakka/csv/scaladsl/CsvFramingSpec.scala) { #line-scanner }

Java
: @@snip (../../../../csv/src/test/java/akka/stream/alpakka/csv/javadsl/CsvFramingTest.java) { #line-scanner }
