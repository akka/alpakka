# Extensible Markup Language - XML

XML parsing module offers Flows for parsing, processing and writing XML documents.


@@project-info{ projectId="xml" }

## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-xml_$scala.binary.version$
  version=$project.version$
  symbol2=AkkaVersion
  value2=$akka.version$
  group2=com.typesafe.akka
  artifact2=akka-stream_$scala.binary.version$
  version2=AkkaVersion
}

The table below shows direct dependencies of this module and the second tab shows all libraries it depends on transitively.

@@dependencies { projectId="xml" }


## XML parsing

XML processing pipeline starts with an @apidoc[XmlParsing.parser](XmlParsing$) flow which parses a stream of @apidoc[akka.util.ByteString]s to XML parser events.

Scala
: @@snip [snip](/xml/src/test/scala/docs/scaladsl/XmlProcessingSpec.scala) { #parser }

Java
: @@snip [snip](/xml/src/test/java/docs/javadsl/XmlParsingTest.java) { #parser }

To parse an XML document run XML document source with this parser.

Scala
: @@snip [snip](/xml/src/test/scala/docs/scaladsl/XmlProcessingSpec.scala) { #parser-usage }

Java
: @@snip [snip](/xml/src/test/java/docs/javadsl/XmlParsingTest.java) { #parser-usage }

To make sense of the parser events, `statefulMapConcat` may be used to aggregate consecutive events and emit the relevant data. For more complex uses, a state machine will be required.

Scala
: @@snip [snip](/xml/src/test/scala/docs/scaladsl/XmlProcessingSpec.scala) { #parser-to-data }

Java
: @@snip [snip](/xml/src/test/java/docs/javadsl/XmlParsingTest.java) { #parser-to-data }


## XML writing

XML processing pipeline ends with an @apidoc[XmlWriting.writer](XmlWriting$) flow which writes a stream of XML parser events to @apidoc[akka.util.ByteString]s.

Scala
: @@snip [snip](/xml/src/test/scala/docs/scaladsl/XmlWritingSpec.scala) { #writer }

Java
: @@snip [snip](/xml/src/test/java/docs/javadsl/XmlWritingTest.java) { #writer }

To write an XML document run XML document source with this writer.

Scala
: @@snip [snip](/xml/src/test/scala/docs/scaladsl/XmlWritingSpec.scala) { #writer-usage }

Java
: @@snip [snip](/xml/src/test/java/docs/javadsl/XmlWritingTest.java) { #writer-usage }

## XML Subslice

Use @apidoc[XmlParsing.subslice](XmlParsing$) to filter out all elements not corresponding to a certain path.

Scala
: @@snip [snip](/xml/src/test/scala/docs/scaladsl/XmlSubsliceSpec.scala) { #subslice }

Java
: @@snip [snip](/xml/src/test/java/docs/javadsl/XmlParsingTest.java) { #subslice }

To get a subslice of an XML document run XML document source with this parser.

Scala
: @@snip [snip](/xml/src/test/scala/docs/scaladsl/XmlSubsliceSpec.scala) { #subslice-usage }

Java
: @@snip [snip](/xml/src/test/java/docs/javadsl/XmlParsingTest.java) { #subslice-usage }

## XML Subtree

Use @apidoc[XmlParsing.subtree](XmlParsing$) to handle elements matched to a certain path and their child nodes as `org.w3c.dom.Element`.

Scala
: @@snip [snip](/xml/src/test/scala/docs/scaladsl/XmlSubtreeSpec.scala) { #subtree }

Java
: @@snip [snip](/xml/src/test/java/docs/javadsl/XmlParsingTest.java) { #subtree }

To get a subtree of an XML document run XML document source with this parser.

Scala
: @@snip [snip](/xml/src/test/scala/docs/scaladsl/XmlSubtreeSpec.scala) { #subtree-usage }

Java
: @@snip [snip](/xml/src/test/java/docs/javadsl/XmlParsingTest.java) { #subtree-usage }


