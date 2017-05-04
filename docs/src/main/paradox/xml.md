# Extensible Markup Language - XML

XML parsing module offers Flows for parsing and processing parsed XML documents.

## Artifacts

sbt
:   @@@vars
    ```scala
    libraryDependencies += "com.lightbend.akka" %% "akka-stream-alpakka-xml" % "$version$"
    ```
    @@@

Maven
:   @@@vars
    ```xml
    <dependency>
      <groupId>com.lightbend.akka</groupId>
      <artifactId>akka-stream-alpakka-xml_$scalaBinaryVersion$</artifactId>
      <version>$version$</version>
    </dependency>
    ```
    @@@

Gradle
:   @@@vars
    ```gradle
    dependencies {
      compile group: "com.lightbend.akka", name: "akka-stream-alpakka-xml_$scalaBinaryVersion$", version: "$version$"
    }
    ```
    @@@

## XML parsing

XML processing pipeline starts with an @scaladoc[XmlParsing.parser](akka.stream.alpakka.xml.scaladsl.XmlParsing$) flow which parses a stream of @scaladoc[ByteString](akka.util.ByteString)s to XML parser events.

Scala
: @@snip (../../../../xml/src/test/scala/akka/stream/alpakka/xml/scaladsl/XmlProcessingTest.scala) { #parser }

Java
: @@snip (../../../../xml/src/test/java/akka/stream/alpakka/xml/javadsl/XmlParsingTest.java) { #parser }

To parse an XML document run XML document source with this parser.

Scala
: @@snip (../../../../xml/src/test/scala/akka/stream/alpakka/xml/scaladsl/XmlProcessingTest.scala) { #parser-usage }

Java
: @@snip (../../../../xml/src/test/java/akka/stream/alpakka/xml/javadsl/XmlParsingTest.java) { #parser-usage }

## XML Subslice

Use @scaladoc[XmlParsing.subslice](akka.stream.alpakka.xml.scaladsl.XmlParsing$) to filter out all elements not corresponding to a certain path.


Scala
: @@snip (../../../../xml/src/test/scala/akka/stream/alpakka/xml/scaladsl/XmlSubsliceTest.scala) { #subslice }

Java
: @@snip (../../../../xml/src/test/java/akka/stream/alpakka/xml/javadsl/XmlParsingTest.java) { #subslice }

To get a subslice of an XML document run XML document source with this parser.

Scala
: @@snip (../../../../xml/src/test/scala/akka/stream/alpakka/xml/scaladsl/XmlSubsliceTest.scala) { #subslice-usage }

Java
: @@snip (../../../../xml/src/test/java/akka/stream/alpakka/xml/javadsl/XmlParsingTest.java) { #subslice-usage }
