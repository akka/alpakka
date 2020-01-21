/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import akka.actor.ActorSystem
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import akka.stream.{ActorMaterializer, Materializer}
import akka.stream.alpakka.xml.scaladsl.XmlParsing
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.util.ByteString
import docs.javadsl.XmlHelper
import org.scalatest.BeforeAndAfterAll

import scala.concurrent.Await
import scala.concurrent.duration._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class XmlSubtreeSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll with LogCapturing {
  implicit val system: ActorSystem = ActorSystem("Test")
  implicit val mat: Materializer = ActorMaterializer()

  //#subtree
  val parse = Flow[String]
    .map(ByteString(_))
    .via(XmlParsing.parser)
    .via(XmlParsing.subtree("doc" :: "elem" :: "item" :: Nil))
    .toMat(Sink.seq)(Keep.right)
  //#subtree

  "XML subtree support" must {

    "properly extract subtree of events" in {
      val doc =
        """
          |<doc>
          |  <elem>
          |    <item>i1</item>
          |    <item>i2</item>
          |    <item>i3</item>
          |  </elem>
          |</doc>
        """.stripMargin

      val result = Await.result(Source.single(doc).runWith(parse), 3.seconds)

      result.map(XmlHelper.asString(_).trim) should ===(
        Seq(
          "<item>i1</item>",
          "<item>i2</item>",
          "<item>i3</item>"
        )
      )
    }

    "properly extract subtree of nested events" in {

      //#subtree-usage
      val doc =
        """
          |<doc>
          |  <elem>
          |    <item>i1</item>
          |    <item><sub>i2</sub></item>
          |    <item>i3</item>
          |  </elem>
          |</doc>
        """.stripMargin
      val resultFuture = Source.single(doc).runWith(parse)
      //#subtree-usage

      val result = Await.result(resultFuture, 3.seconds)

      result.map(XmlHelper.asString(_).trim) should ===(
        Seq(
          "<item>i1</item>",
          "<item><sub>i2</sub></item>",
          "<item>i3</item>"
        )
      )
    }

    "properly ignore matches not deep enough" in {
      val doc =
        """
          |<doc>
          |  <elem>
          |     I am lonely here :(
          |  </elem>
          |</doc>
        """.stripMargin

      val result = Await.result(Source.single(doc).runWith(parse), 3.seconds)
      result should ===(Nil)
    }

    "properly ignore partial matches" in {
      val doc =
        """
          |<doc>
          |  <elem>
          |     <notanitem>ignore me</notanitem>
          |     <notanitem>ignore me</notanitem>
          |     <foo>ignore me</foo>
          |  </elem>
          |  <bar></bar>
          |</doc>
        """.stripMargin

      val result = Await.result(Source.single(doc).runWith(parse), 3.seconds)
      result should ===(Nil)
    }

    "properly filter from the combination of the above" in {
      val doc =
        """
          |<doc>
          |  <elem>
          |    <notanitem>ignore me</notanitem>
          |    <notanitem>ignore me</notanitem>
          |    <foo>ignore me</foo>
          |    <item>i1</item>
          |    <item><sub>i2</sub></item>
          |    <item>i3</item>
          |  </elem>
          |  <elem>
          |    not me please
          |  </elem>
          |  <elem><item>i4</item></elem>
          |</doc>
        """.stripMargin

      val result = Await.result(Source.single(doc).runWith(parse), 3.seconds)

      result.map(XmlHelper.asString(_).trim) should ===(
        Seq(
          "<item>i1</item>",
          "<item><sub>i2</sub></item>",
          "<item>i3</item>",
          "<item>i4</item>"
        )
      )
    }

    "properly extract a subtree of events even with the namespace prefix" in {
      val doc =
        """
          |<doc xmlns:g="http://base.google.com/ns/1.0" version="2.0">
          | <elem>
          |   <item><g:id>id1</g:id></item>
          |	</elem>
          |</doc>
        """.stripMargin

      val result = Await.result(Source.single(doc).runWith(parse), 3.seconds)

      val t = result.map(XmlHelper.asString(_).trim)
      val f = Seq("""<item><id xmlns="http://base.google.com/ns/1.0">id1</id></item>""".stripMargin)
      t should ===(f)
    }

  }

  override protected def afterAll(): Unit = system.terminate()
}
