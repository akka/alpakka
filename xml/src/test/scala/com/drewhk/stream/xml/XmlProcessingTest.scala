/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package com.drewhk.stream.xml

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{ Flow, Keep, Sink, Source }
import akka.util.ByteString
import com.drewhk.stream.xml.Xml._
import org.scalatest.{ BeforeAndAfterAll, Matchers, WordSpec }

import scala.collection.immutable
import scala.concurrent.Await
import scala.concurrent.duration._

class XmlProcessingTest extends WordSpec with Matchers with BeforeAndAfterAll {
  implicit val system = ActorSystem("Test")
  implicit val mat = ActorMaterializer()

  val parse = Flow[String].map(ByteString(_))
    .via(Xml.parser)
    .toMat(Sink.seq)(Keep.right)

  "XML Parser" must {

    "properly parse simple XML" in {
      val doc = "<doc><elem>elem1</elem><elem>elem2</elem></doc>"

      val resultFuture = Source.single(doc).runWith(parse)

      val result = Await.result(resultFuture, 3.seconds)
      result should ===(List(
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
      ))
    }

    "properly process a comment" in {
      val doc = "<doc><!--comment--></doc>"

      val resultFuture = Source.single(doc).runWith(parse)

      val result = Await.result(resultFuture, 3.seconds)
      result should ===(List(
        StartDocument,
        StartElement("doc", Map.empty),
        Comment("comment"),
        EndElement("doc"),
        EndDocument
      ))
    }

    "properly process parse instructions" in {
      val doc = """<?target content?><doc></doc>"""

      val resultFuture = Source.single(doc).runWith(parse)

      val result = Await.result(resultFuture, 3.seconds)
      result should ===(List(
        StartDocument,
        ProcessingInstruction(Some("target"), Some("content")),
        StartElement("doc", Map.empty),
        EndElement("doc"),
        EndDocument
      ))

    }

    "properly process attributes" in {
      val doc = """<doc good="yes"><elem nice="yes" very="true">elem1</elem></doc>"""

      val resultFuture = Source.single(doc).runWith(parse)

      val result = Await.result(resultFuture, 3.seconds)
      result should ===(List(
        StartDocument,
        StartElement("doc", Map("good" -> "yes")),
        StartElement("elem", Map("nice" -> "yes", "very" -> "true")),
        Characters("elem1"),
        EndElement("elem"),
        EndElement("doc"),
        EndDocument
      ))
    }

    "properly process CData blocks" in {
      val doc = """<doc><![CDATA[<not>even</valid>]]></doc>"""

      val resultFuture = Source.single(doc).runWith(parse)

      val result = Await.result(resultFuture, 3.seconds)
      result should ===(List(
        StartDocument,
        StartElement("doc", Map.empty),
        CData("<not>even</valid>"),
        EndElement("doc"),
        EndDocument
      ))
    }

    "properly parse large XML" in {
      val elements = immutable.Iterable.range(0, 10).map(_.toString)

      val documentStream =
        Source.single("<doc>")
          .concat(Source(elements).intersperse("<elem>", "</elem><elem>", "</elem>"))
          .concat(Source.single("</doc>"))

      val resultFuture = documentStream.map(ByteString(_))
        .via(Xml.parser)
        .filter(_ match {
          case EndDocument        => false
          case StartDocument      => false
          case EndElement("elem") => false
          case _                  => true
        }).splitWhen(_ match {
          case StartElement("elem", _) => true
          case _                       => false
        }).collect({
          case Characters(s) => s
        }).concatSubstreams
        .runWith(Sink.seq)

      val result = Await.result(resultFuture, 3.seconds)
    }

  }

  override protected def afterAll(): Unit = system.terminate()
}
