/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
//#run-via-scanner
import akka.stream.alpakka.recordio.scaladsl.RecordIOFraming
//#run-via-scanner
import akka.stream.scaladsl.Framing.FramingException
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.testkit.TestKit
import akka.util.ByteString
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.BeforeAndAfterAll

import scala.collection.immutable.Seq
import scala.concurrent.Future
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

class RecordIOFramingSpec(_system: ActorSystem)
    extends TestKit(_system)
    with AnyFlatSpecLike
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll
    with LogCapturing {
  def this() = this(ActorSystem("RecordIOFramingSpec"))

  override protected def afterAll(): Unit = shutdown()

  //#run-via-scanner

  val FirstRecordData =
    """{"type": "SUBSCRIBED","subscribed": {"framework_id": {"value":"12220-3440-12532-2345"},"heartbeat_interval_seconds":15.0}"""
  val SecondRecordData = """{"type":"HEARTBEAT"}"""

  val FirstRecordWithPrefix = s"121\n$FirstRecordData"
  val SecondRecordWithPrefix = s"20\n$SecondRecordData"

  val basicSource: Source[ByteString, NotUsed] =
    Source.single(ByteString(FirstRecordWithPrefix + SecondRecordWithPrefix))

  //#run-via-scanner

  val stringSeqSink = Flow[ByteString].map(_.utf8String).toMat(Sink.seq)(Keep.right)

  "RecordIO framing" should "parse a series of records" in {
    // When
    //#run-via-scanner
    val result: Future[Seq[ByteString]] = basicSource
      .via(RecordIOFraming.scanner())
      .runWith(Sink.seq)
    //#run-via-scanner

    // Then
    //#result
    val byteStrings = result.futureValue

    byteStrings(0) shouldBe ByteString(FirstRecordData)
    byteStrings(1) shouldBe ByteString(SecondRecordData)
    //#result
  }

  it should "parse input with additional whitespace" in {
    // Given
    val recordIOInput = s"\t\n $FirstRecordWithPrefix \r\n $SecondRecordWithPrefix \t"

    // When
    val result = Source.single(ByteString(recordIOInput)) via
      RecordIOFraming.scanner() runWith
      stringSeqSink

    // Then
    result.futureValue shouldBe Seq(FirstRecordData, SecondRecordData)
  }

  it should "parse a chunked stream" in {
    // Given
    val whitespaceChunk = "\n\n"
    val (secondChunk, thirdChunk) = s"\n\n$FirstRecordWithPrefix\n\n".splitAt(50)
    val (fifthChunk, sixthChunk) = s"\n\n$SecondRecordWithPrefix\n\n".splitAt(10)

    val chunkedInput =
      Seq(whitespaceChunk, secondChunk, thirdChunk, whitespaceChunk, fifthChunk, sixthChunk, whitespaceChunk).map(
        ByteString(_)
      )

    // When
    val result = Source(chunkedInput) via
      RecordIOFraming.scanner() runWith
      stringSeqSink

    // Then
    result.futureValue shouldBe Seq(FirstRecordData, SecondRecordData)
  }

  it should "handle an empty stream" in {
    // When
    val result =
      Source.empty via
      RecordIOFraming.scanner() runWith
      stringSeqSink

    // Then
    result.futureValue shouldBe Seq.empty
  }

  it should "handle a stream containing only whitespace" in {
    // Given
    val input = Seq("\n\n", "  ", "\t\t").map(ByteString(_))

    // When
    val result = Source(input) via
      RecordIOFraming.scanner() runWith
      stringSeqSink

    // Then
    result.futureValue shouldBe Seq.empty
  }

  it should "reject an unparseable record size prefix" in {
    // Given
    val recordIOInput = s"NAN\n$FirstRecordData"

    // When
    val result = Source.single(ByteString(recordIOInput)) via
      RecordIOFraming.scanner(1024) runWith
      stringSeqSink

    // Then
    result.failed.futureValue shouldBe a[NumberFormatException]
  }

  it should "reject an overly long record size prefix" in {
    // Given
    val infinitePrefixSource = Source.repeat(ByteString("1"))

    // When
    val result = infinitePrefixSource via
      RecordIOFraming.scanner(1024) runWith
      stringSeqSink

    // Then
    result.failed.futureValue shouldBe a[FramingException]
  }

  it should "reject a negative record size prefix" in {
    // Given
    val recordIOInput = s"-1\n"

    // When
    val result = Source.single(ByteString(recordIOInput)) via
      RecordIOFraming.scanner() runWith
      stringSeqSink

    // Then
    result.failed.futureValue shouldBe a[FramingException]
  }

  it should "reject an overly long record" in {
    // Given
    val recordIOInput = FirstRecordWithPrefix

    // When
    val result = Source.single(ByteString(recordIOInput)) via
      RecordIOFraming.scanner(FirstRecordData.length - 1) runWith
      stringSeqSink

    // Then
    result.failed.futureValue shouldBe a[FramingException]
  }

  it should "reject a truncated record" in {
    // Given
    val recordIOInput = FirstRecordWithPrefix.dropRight(1)

    // When
    val result = Source.single(ByteString(recordIOInput)) via
      RecordIOFraming.scanner() runWith
      stringSeqSink

    // Then
    result.failed.futureValue shouldBe a[FramingException]
  }
}
