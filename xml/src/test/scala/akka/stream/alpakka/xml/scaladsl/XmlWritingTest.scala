/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.xml.scaladsl

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.xml._
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

import scala.concurrent.Future
import scala.concurrent.duration._

class XmlWritingTest extends WordSpec with Matchers with BeforeAndAfterAll with ScalaFutures {
  implicit val system = ActorSystem("Test")
  implicit val mat = ActorMaterializer()

  // #writer
  val writer: Sink[ParseEvent, Future[String]] = Flow[ParseEvent]
    .via(XmlWriting.writer)
    .map[String](_.utf8String)
    .toMat(Sink.fold[String, String]("")((t, u) => t + u))(Keep.right)
  // #writer

  "XML Writer" must {

    "properly write simple XML" in {
      // #writer-usage
      val listEl: List[ParseEvent] = List(
        StartDocument,
        StartElement("doc", Map.empty),
        StartElement("elem", Map.empty),
        Characters("elem1"),
        EndElement("elem"),
        StartElement("elem", Map.empty),
        Characters("elem2"),
        EndElement("elem"),
        EndElement("doc"),
        EndDocument
      )

      val doc = "<?xml version='1.0' encoding='UTF-8'?><doc><elem>elem1</elem><elem>elem2</elem></doc>"
      val resultFuture: Future[String] = Source.fromIterator[ParseEvent](() => listEl.iterator).runWith(writer)
      // #writer-usage

      resultFuture.futureValue(Timeout(20.seconds)) should ===(doc)
    }

    "properly process a comment" in {
      val doc = "<?xml version='1.0' encoding='UTF-8'?><doc><!--comment--></doc>"
      val listEl = List(
        StartDocument,
        StartElement("doc", Map.empty),
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
        StartElement("doc", Map.empty),
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
        StartElement("doc", Map.empty),
        CData("<not>even</valid>"),
        EndElement("doc"),
        EndDocument
      )
      val resultFuture: Future[String] = Source.fromIterator[ParseEvent](() => listEl.iterator).runWith(writer)

      resultFuture.futureValue(Timeout(3.seconds)) should ===(doc)
    }

  }

  override protected def afterAll(): Unit = system.terminate()
}
