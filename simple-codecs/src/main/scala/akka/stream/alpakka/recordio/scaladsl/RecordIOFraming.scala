/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.recordio.scaladsl

import akka.NotUsed
import akka.stream.Attributes.name
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Framing.FramingException
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.util.ByteString

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

// Provides a JSON framing flow that can separate records from an incoming RecordIO-formatted [[ByteString]] stream.
object RecordIOFraming {

  /**
   * Returns a flow that parses an incoming RecordIO stream and emits the identified records.
   *
   * The incoming stream is expected to be a concatenation of records of the format:
   *
   *   [record length]\n[record data]
   *
   * The parser ignores whitespace before or after each record. It is agnostic to the record data contents.
   *
   * The flow will emit each record's data as a byte string.
   *
   * @param maxRecordLength The maximum record length allowed. If a record is indicated to be longer, this Flow will fail the stream.
   */
  def scanner(maxRecordLength: Int = Int.MaxValue): Flow[ByteString, ByteString, NotUsed] =
    Flow[ByteString].via(new RecordIOFramingStage(maxRecordLength)).named("recordIOFraming")

  private val LineFeed = '\n'.toByte
  private val CarriageReturn = '\r'.toByte
  private val Tab = '\t'.toByte
  private val Space = ' '.toByte

  private val Whitespace: Set[Byte] = Set(LineFeed, CarriageReturn, Tab, Space)

  private def isWhitespace(byte: Byte): Boolean = Whitespace.contains(byte)

  private class RecordIOFramingStage(maxRecordLength: Int) extends GraphStage[FlowShape[ByteString, ByteString]] {

    val in = Inlet[ByteString]("RecordIOFramingStage.in")
    val out = Outlet[ByteString]("RecordIOFramingStage.out")
    override val shape: FlowShape[ByteString, ByteString] = FlowShape(in, out)

    override def initialAttributes: Attributes = name("recordIOFraming")
    override def toString: String = "RecordIOFraming"

    // The maximum length of the record prefix indicating its size.
    private val maxRecordPrefixLength = maxRecordLength.toString.length

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) with InHandler with OutHandler {
        private var buffer = ByteString.empty

        private def trimWhitespace(): Unit = buffer = buffer.dropWhile(isWhitespace)

        private var currentRecordLength: Option[Int] = None // the byte length of the next record, if known

        override def onPush(): Unit = {
          buffer ++= grab(in)
          doParse()
        }

        override def onPull(): Unit = doParse()

        override def onUpstreamFinish(): Unit =
          if (buffer.isEmpty) {
            completeStage()
          } else if (isAvailable(out)) {
            doParse()
          } // else swallow the termination and wait for pull

        private def tryPull(): Unit =
          if (isClosed(in)) {
            failStage(new FramingException("Stream finished but there was a truncated final record in the buffer."))
          } else pull(in)

        @tailrec
        private def doParse(): Unit =
          currentRecordLength match {
            case Some(length) if buffer.size >= length =>
              val (record, buf) = buffer.splitAt(length)
              buffer = buf.compact
              trimWhitespace()

              currentRecordLength = None

              push(out, record.compact)
            case Some(_) =>
              tryPull()
            case None =>
              trimWhitespace()
              buffer.indexOf(LineFeed) match {
                case -1 if buffer.size > maxRecordPrefixLength =>
                  failStage(new FramingException(s"Record size prefix is longer than $maxRecordPrefixLength bytes."))
                case -1 if isClosed(in) && buffer.isEmpty =>
                  completeStage()
                case -1 =>
                  tryPull()
                case lfPos =>
                  val (recordSizePrefix, buf) = buffer.splitAt(lfPos)
                  buffer = buf.drop(1).compact

                  Try(recordSizePrefix.utf8String.toInt) match {
                    case Success(length) if length > maxRecordLength =>
                      failStage(
                        new FramingException(
                          s"Record of size $length bytes exceeds maximum of $maxRecordLength bytes."
                        )
                      )
                    case Success(length) if length < 0 =>
                      failStage(new FramingException(s"Record size prefix $length is negative."))
                    case Success(length) =>
                      currentRecordLength = Some(length)
                      doParse()
                    case Failure(ex) =>
                      failStage(ex)
                  }
              }
          }

        setHandlers(in, out, this)
      }
  }
}
