# XML Connector

The XML connector provides Akka Stream sources and sinks to parse XML documents and validate XML documents.

## Artifacts

sbt
:   @@@vars
    ```scala
    libraryDependencies += "com.typesafe.akka" %% "akka-stream-alpakka-xml" % "$version$"
    ```
    @@@

Maven
:   @@@vars
    ```xml
    <dependency>
      <groupId>com.typesafe.akka</groupId>
      <artifactId>akka-stream-alpakka-xml$scala.binaryVersion$</artifactId>
      <version>$version$</version>
    </dependency>
    ```
    @@@

Gradle
:   @@@vars
    ```gradle
    dependencies {
      compile group: "com.typesafe.akka", name: "akka-stream-alpakka-xml_$scala.binaryVersion$", version: "$version$"
    }
    ```
    @@@

## Components
The XML connector provides the following components

## akka.stream.alpakka.xml.XsdValidator
Given a stream of ByteString, it validates an XML file given an XSD that is read from the classpath. It returns a [[XsdValidationResult]] containing
the success or failure of the validated XML stream.

For example lets validate the file `/tmp/people.xml` against a classpath resource `xsd/people.xsd`:

```scala
import java.nio.file.Paths
import akka.stream.scaladsl.FileIO
import scala.concurrent.Future

val f: Future[XsdValidationResult] =
  FileIO.fromPath(Paths.get("/tmp/people.xml")).runWith(XsdValidation.sink("xsd/people.xsd"))
```

## akka.stream.alpakka.xml.XmlEventSource
Given an `java.io.InputStream` or `filename` or `java.io.File` it creates a `Source[XMLEvent, NotUsed]` that can be used to process an XML file.

## akka.stream.alpakka.xml.XmlParser
It should be easy to write XML parsers to process large XML files efficiently. Most often this means reading the XML
sequentially, parsing a known XML fragment and converting it to DTOs using case classes. For such a use case the
XmlParser should help you get you up and running fast!

For example, let's process the following XML:

```xml
<orders>
    <order id="1">
        <item name="Pizza" price="12.00">
            <pizza>
                <crust type="thin" size="14"/>
                <topping>cheese</topping>
                <topping>sausage</topping>
            </pizza>
        </item>
        <item name="Breadsticks" price="4.00"/>
        <tax type="federal">0.80</tax>
        <tax type="state">0.80</tax>
        <tax type="local">0.40</tax>
    </order>
</orders>
```

Imagine we are interested in only `orders`, and only the `tax`, lets write two parsers:

```scala
import scala.xml.pull._
import akka.stream.scaladsl._
import akka.stream.alpakka.xml.XmlParser
import akka.stream.alpakka.xml.XmlParser._
import akka.stream.alpakka.xml.XmlEventSource

case class Order(id: String)

val orderParser: Flow[XMLEvent, Order] = {
 var orderId: String = null
 XmlParser.flow {
  case EvElemStart(_, "order", meta, _) ⇒
    orderId = getAttr(meta)("id"); emit()
  case EvElemEnd(_, "order") ⇒
    emit(Order(orderId))
 }
}

case class Tax(taxType: String, value: String)

val tagParser: Flow[XMLEvent, Tax] = {
  var taxType: String = null
  var taxValue: String = null
  XmlParser.flow {
    case EvElemStart(_, "tax", meta, _) =>
      taxType = getAttr(meta)("type"); emit()
    case EvText(text) ⇒
      taxValue = text; emit()
    case EvElemEnd(_, "tax") ⇒
      emit(Tax(taxType, taxValue))
  }
}

XmlEventSource.fromFileName("orders.xml")
 .via(orderParser).runForeach(println)

XmlEventSource.fromFileName("orders.xml")
 .via(tagParser).runForeach(println)
```

For a more complex example that parses a nested document lets look at parsing a person document:

@@snip[person.xsd](/../../../../xml/src/test/resources/xml/people.xsd)

The parser implementation for this schema:

@@snip[person.xsd](/../../../../xml/src/test/scala/akka/stream/alpakka/xml/PersonParser.scala)

## Testing


Scala
:   ```
    sbt
    > xml/test
    ```

Java
:   ```
    sbt
    > xml/test
    ```
