/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import akka.stream.alpakka.xml._
import akka.stream.alpakka.xml.scaladsl.XmlWriting
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

import scala.concurrent.Future
import scala.concurrent.duration._

class XmlWritingSpec extends WordSpec with Matchers with BeforeAndAfterAll with ScalaFutures {
  implicit val system: ActorSystem = ActorSystem("Test")
  implicit val mat: Materializer = ActorMaterializer()

  // #writer
  val writer: Sink[ParseEvent, Future[String]] = Flow[ParseEvent]
    .via(XmlWriting.writer)
    .map[String](_.utf8String)
    .toMat(Sink.fold[String, String]("")((t, u) => t + u))(Keep.right)
  // #writer

  "XML Writer" must {

    "properly write simple XML" in {
      val listEl: List[ParseEvent] = List(
        StartDocument,
        StartElement("doc"),
        StartElement("elem"),
        Characters("elem1"),
        EndElement("elem"),
        StartElement("elem"),
        Characters("elem2"),
        EndElement("elem"),
        EndElement("doc"),
        EndDocument
      )

      val doc = "<?xml version='1.0' encoding='UTF-8'?><doc><elem>elem1</elem><elem>elem2</elem></doc>"
      val resultFuture: Future[String] = Source.fromIterator[ParseEvent](() => listEl.iterator).runWith(writer)

      resultFuture.futureValue(Timeout(20.seconds)) should ===(doc)
    }

    "properly process a comment" in {
      val doc = "<?xml version='1.0' encoding='UTF-8'?><doc><!--comment--></doc>"
      val listEl = List(
        StartDocument,
        StartElement("doc"),
        Comment("comment"),
        EndElement("doc"),
        EndDocument
      )

      val resultFuture: Future[String] = Source.fromIterator[ParseEvent](() => listEl.iterator).runWith(writer)

      resultFuture.futureValue(Timeout(20.seconds)) should ===(doc)
    }

    "properly process parse instructions" in {
      val doc = """<?xml version='1.0' encoding='UTF-8'?><?target content?><doc/>"""
      val listEl = List(
        StartDocument,
        ProcessingInstruction(Some("target"), Some("content")),
        StartElement("doc"),
        EndElement("doc"),
        EndDocument
      )

      val resultFuture: Future[String] = Source.fromIterator[ParseEvent](() => listEl.iterator).runWith(writer)

      resultFuture.futureValue(Timeout(20.seconds)) should ===(doc)

    }

    "properly process attributes" in {
      val doc =
        """<?xml version='1.0' encoding='UTF-8'?><doc good="yes"><elem nice="yes" very="true">elem1</elem></doc>"""
      val listEl = List(
        StartDocument,
        StartElement("doc", Map("good" -> "yes")),
        StartElement("elem", Map("nice" -> "yes", "very" -> "true")),
        Characters("elem1"),
        EndElement("elem"),
        EndElement("doc"),
        EndDocument
      )

      val resultFuture: Future[String] = Source.fromIterator[ParseEvent](() => listEl.iterator).runWith(writer)

      resultFuture.futureValue(Timeout(3.seconds)) should ===(doc)
    }

    "properly process CData blocks" in {
      val doc = """<?xml version='1.0' encoding='UTF-8'?><doc><![CDATA[<not>even</valid>]]></doc>"""

      val listEl = List(
        StartDocument,
        StartElement("doc"),
        CData("<not>even</valid>"),
        EndElement("doc"),
        EndDocument
      )
      val resultFuture: Future[String] = Source.fromIterator[ParseEvent](() => listEl.iterator).runWith(writer)

      resultFuture.futureValue(Timeout(3.seconds)) should ===(doc)
    }

    "properly process with default namespace" in {
      val doc =
        """<?xml version='1.0' encoding='UTF-8'?><doc xmlns="test:xml:0.1"><elem>elem1</elem><elem>elem2</elem></doc>"""

      val listEl = List(
        StartDocument,
        StartElement("doc", namespace = Some("test:xml:0.1"), namespaceCtx = List(Namespace("test:xml:0.1"))),
        StartElement("elem", namespace = Some("test:xml:0.1")),
        Characters("elem1"),
        EndElement("elem"),
        StartElement("elem", namespace = Some("test:xml:0.1")),
        Characters("elem2"),
        EndElement("elem"),
        EndElement("doc"),
        EndDocument
      )
      val resultFuture: Future[String] = Source.fromIterator[ParseEvent](() => listEl.iterator).runWith(writer)

      resultFuture.futureValue(Timeout(3.seconds)) should ===(doc)
    }

    "properly process prefixed namespaces" in {
      val doc = """<?xml version='1.0' encoding='UTF-8'?><x xmlns:edi="http://ecommerce.example.org/schema"/>"""

      val listEl = List(
        StartDocument,
        StartElement("x",
                     namespace = None,
                     prefix = None,
                     namespaceCtx = List(Namespace("http://ecommerce.example.org/schema", prefix = Some("edi")))),
        EndElement("x"),
        EndDocument
      )
      val resultFuture: Future[String] = Source.fromIterator[ParseEvent](() => listEl.iterator).runWith(writer)

      resultFuture.futureValue(Timeout(3.seconds)) should ===(doc)
    }

    "properly process multiple namespaces" in {
      // #writer-usage
      val listEl = List(
        StartDocument,
        StartElement(
          "book",
          namespace = Some("urn:loc.gov:books"),
          prefix = Some("bk"),
          namespaceCtx = List(Namespace("urn:loc.gov:books", prefix = Some("bk")),
                              Namespace("urn:ISBN:0-395-36341-6", prefix = Some("isbn")))
        ),
        StartElement(
          "title",
          namespace = Some("urn:loc.gov:books"),
          prefix = Some("bk")
        ),
        Characters("Cheaper by the Dozen"),
        EndElement("title"),
        StartElement(
          "number",
          namespace = Some("urn:ISBN:0-395-36341-6"),
          prefix = Some("isbn")
        ),
        Characters("1568491379"),
        EndElement("number"),
        EndElement("book"),
        EndDocument
      )

      val doc =
        """<?xml version='1.0' encoding='UTF-8'?><bk:book xmlns:bk="urn:loc.gov:books" xmlns:isbn="urn:ISBN:0-395-36341-6"><bk:title>Cheaper by the Dozen</bk:title><isbn:number>1568491379</isbn:number></bk:book>"""
      val resultFuture: Future[String] = Source.fromIterator[ParseEvent](() => listEl.iterator).runWith(writer)
      resultFuture.futureValue(Timeout(3.seconds)) should ===(doc)
      // #writer-usage
    }

    "properly process a string that is not a full document" in {
      val listEl: List[ParseEvent] = List(
        StartElement("doc"),
        StartElement("elem"),
        Characters("elem1"),
        EndElement("elem"),
        StartElement("elem"),
        Characters("elem2"),
        EndElement("elem"),
        EndElement("doc")
      )

      val doc = "<doc><elem>elem1</elem><elem>elem2</elem></doc>"
      val resultFuture: Future[String] = Source.fromIterator[ParseEvent](() => listEl.iterator).runWith(writer)

      resultFuture.futureValue(Timeout(20.seconds)) should ===(doc)
    }

  }

  override protected def afterAll(): Unit = system.terminate()
}
