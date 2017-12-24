# Extensible Markup Language - XML

XML parsing module offers Flows for parsing and processing parsed XML documents.

## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-xml_$scalaBinaryVersion$
  version=$version$
}

## XML parsing

XML processing pipeline starts with an @scaladoc[XmlParsing.parser](akka.stream.alpakka.xml.scaladsl.XmlParsing$) flow which parses a stream of @scaladoc[ByteString](akka.util.ByteString)s to XML parser events.

Scala
: @@snip ($alpakka$/xml/src/test/scala/akka/stream/alpakka/xml/scaladsl/XmlProcessingTest.scala) { #parser }

Java
: @@snip ($alpakka$/xml/src/test/java/akka/stream/alpakka/xml/javadsl/XmlParsingTest.java) { #parser }

@scala[@github[Source on Github](/xml/src/test/scala/akka/stream/alpakka/xml/scaladsl/XmlProcessingTest.scala) { #parser }]
@java[@github[Source on Github](/xml/src/test/java/akka/stream/alpakka/xml/javadsl/XmlParsingTest.java) { #parser }]


To parse an XML document run XML document source with this parser.

Scala
: @@snip ($alpakka$/xml/src/test/scala/akka/stream/alpakka/xml/scaladsl/XmlProcessingTest.scala) { #parser-usage }

Java
: @@snip ($alpakka$/xml/src/test/java/akka/stream/alpakka/xml/javadsl/XmlParsingTest.java) { #parser-usage }

## XML writing

XML processing pipeline ends with an @scaladoc[XmlWriting.writer](akka.stream.alpakka.xml.scaladsl.XmlWriting$) flow which writes a stream of XML parser events to @scaladoc[ByteString](akka.util.ByteString)s.

Scala
: @@snip ($alpakka$/xml/src/test/scala/akka/stream/alpakka/xml/scaladsl/XmlWritingTest.scala) { #writer }

Java
: @@snip ($alpakka$/xml/src/test/java/akka/stream/alpakka/xml/javadsl/XmlWritingTest.java) { #writer }

@scala[@github[Source on Github](/xml/src/test/scala/akka/stream/alpakka/xml/scaladsl/XmlWritingTest.scala) { #writer }]
@java[@github[Source on Github](/xml/src/test/java/akka/stream/alpakka/xml/javadsl/XmlWritingTest.java) { #writer }]


To write an XML document run XML document source with this writer.

Scala
: @@snip ($alpakka$/xml/src/test/scala/akka/stream/alpakka/xml/scaladsl/XmlWritingTest.scala) { #writer-usage }

Java
: @@snip ($alpakka$/xml/src/test/java/akka/stream/alpakka/xml/javadsl/XmlWritingTest.java) { #writer-usage }

## XML Subslice

Use @scaladoc[XmlParsing.subslice](akka.stream.alpakka.xml.scaladsl.XmlParsing$) to filter out all elements not corresponding to a certain path.


Scala
: @@snip ($alpakka$/xml/src/test/scala/akka/stream/alpakka/xml/scaladsl/XmlSubsliceTest.scala) { #subslice }

Java
: @@snip ($alpakka$/xml/src/test/java/akka/stream/alpakka/xml/javadsl/XmlParsingTest.java) { #subslice }

@scala[@github[Source on Github](/xml/src/test/scala/akka/stream/alpakka/xml/scaladsl/XmlSubsliceTest.scala) { #subslice }]
@java[@github[Source on Github](/xml/src/test/java/akka/stream/alpakka/xml/javadsl/XmlParsingTest.java) { #subslice }]


To get a subslice of an XML document run XML document source with this parser.

Scala
: @@snip ($alpakka$/xml/src/test/scala/akka/stream/alpakka/xml/scaladsl/XmlSubsliceTest.scala) { #subslice-usage }

Java
: @@snip ($alpakka$/xml/src/test/java/akka/stream/alpakka/xml/javadsl/XmlParsingTest.java) { #subslice-usage }
