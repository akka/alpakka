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


@@project-info{ projectId="csv" }


## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-csv_$scala.binary.version$
  version=$project.version$
  symbol2=AkkaVersion
  value2=$akka.version$
  group2=com.typesafe.akka
  artifact2=akka-stream_$scala.binary.version$
  version2=AkkaVersion
}

The table below shows direct dependencies of this module and the second tab shows all libraries it depends on transitively.

@@dependencies { projectId="csv" }


## CSV parsing

CSV parsing offers a flow that takes a stream of `akka.util.ByteString` and issues a stream of lists of `ByteString`.

The incoming data must contain line ends to allow line base framing. The CSV special characters
can be specified (as bytes), suitable values are available as constants in `CsvParsing`.

@@@ note

The current parser is limited to byte-based character sets (UTF-8, ISO-8859-1, ASCII) and can't
parse double-byte encodings (e.g. UTF-16).

The parser accepts Byte Order Mark (BOM) for UTF-8, but will fail for UTF-16 and UTF-32
Byte Order Marks.

@@@

Scala
: @@snip [snip](/csv/src/test/scala/docs/scaladsl/CsvParsingSpec.scala) { #flow-type }

Java
: @@snip [snip](/csv/src/test/java/docs/javadsl/CsvParsingTest.java) { #import #flow-type }


In this sample we read a single line of CSV formatted data into a list of column elements:

Scala
: @@snip [snip](/csv/src/test/scala/docs/scaladsl/CsvParsingSpec.scala) { #line-scanner }

Java
: @@snip [snip](/csv/src/test/java/docs/javadsl/CsvParsingTest.java) { #import #line-scanner }

To convert the `ByteString` columns as `String`, a `map` operation can be added to the Flow:

Scala
: @@snip [snip](/csv/src/test/scala/docs/scaladsl/CsvParsingSpec.scala) { #line-scanner-string }

Java
: @@snip [snip](/csv/src/test/java/docs/javadsl/CsvParsingTest.java) { #import #line-scanner-string }

## CSV conversion into a map

The column-based nature of CSV files can be used to read it into a map of column names
and their `ByteString` values, or alternatively to `String` values. The column names can be either provided in code or 
the first line of data can be interpreted as the column names.

Scala
: @@snip [snip](/csv/src/test/scala/docs/scaladsl/CsvToMapSpec.scala) { #flow-type }

Java
: @@snip [snip](/csv/src/test/java/docs/javadsl/CsvToMapTest.java) { #import #flow-type }


This example uses the first line (the header line) in the CSV data as column names:

Scala
: @@snip [snip](/csv/src/test/scala/docs/scaladsl/CsvToMapSpec.scala) { #header-line }

Java
: @@snip [snip](/csv/src/test/java/docs/javadsl/CsvToMapTest.java) { #import #header-line }


This sample will generate the same output as above, but the column names are specified
in the code:

Scala
: @@snip [snip](/csv/src/test/scala/docs/scaladsl/CsvToMapSpec.scala) { #column-names }

Java
: @@snip [snip](/csv/src/test/java/docs/javadsl/CsvToMapTest.java) { #import #column-names }

## CSV formatting

To emit CSV files ``immutable.Seq[String]`` can be formatted into ``ByteString`` e.g to be written to file.
The formatter takes care of quoting and escaping.

Certain CSV readers (e.g. Microsoft Excel) require CSV files to indicate their character encoding with a *Byte
Order Mark* (BOM) in the first bytes of the file. Choose an appropriate Byte Order Mark matching the
selected character set from the constants in `ByteOrderMark`
([Unicode FAQ on Byte Order Mark](http://www.unicode.org/faq/utf_bom.html#bom1)).


Scala
: @@snip [snip](/csv/src/test/scala/docs/scaladsl/CsvFormattingSpec.scala) { #flow-type }

Java
: @@snip [snip](/csv/src/test/java/docs/javadsl/CsvFormattingTest.java) { #import #flow-type }

This example uses the default configuration:

- Delimiter: comma (,)
- Quote char: double quote (")
- Escape char: backslash (\\)
- Line ending: Carriage Return and Line Feed (`\r` = ASCII 13 + `\n` = ASCII 10)
- Quoting style: quote only if required
- Charset: UTF-8
- No Byte Order Mark

Scala
: @@snip [snip](/csv/src/test/scala/docs/scaladsl/CsvFormattingSpec.scala) { #formatting }

Java
: @@snip [snip](/csv/src/test/java/docs/javadsl/CsvFormattingTest.java) { #import #formatting }
