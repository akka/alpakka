# Comma-Separated Values - CSV

Comma-Separated Values are used as interchange format for tabular data 
of text. This format is supported by most spreadsheet applications and may 
be used as database extraction format.

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

The incoming data must contain line ends to allow line base framing. The CSV special characters
can be specified (as bytes), suitable values are available as constants in `CsvFraming`.

Scala
: @@snip (../../../../csv/src/test/scala/akka/stream/alpakka/csv/scaladsl/CsvFramingSpec.scala) { #flow-type }

Java
: @@snip (../../../../csv/src/test/java/akka/stream/alpakka/csv/javadsl/CsvFramingTest.java) { #flow-type }

In this sample we read a single line of CSV formatted data into a list of column elements:

Scala
: @@snip (../../../../csv/src/test/scala/akka/stream/alpakka/csv/scaladsl/CsvFramingSpec.scala) { #line-scanner }

Java
: @@snip (../../../../csv/src/test/java/akka/stream/alpakka/csv/javadsl/CsvFramingTest.java) { #line-scanner }

### CSV parsing and conversion into a map

The column-based nature of CSV files can be used to read it into a map of column names 
and their `ByteStrng` values. The column names can be either provided in code or the first line 
of data can be interpreted as the column names.

This sample uses the first line in the CSV data as column names:

Scala
: @@snip (../../../../csv/src/test/scala/akka/stream/alpakka/csv/scaladsl/CsvToMapSpec.scala) { #header-line }

This sample will generate the same output as above, but the column names are specified
in code:

Scala
: @@snip (../../../../csv/src/test/scala/akka/stream/alpakka/csv/scaladsl/CsvToMapSpec.scala) { #column-names }

