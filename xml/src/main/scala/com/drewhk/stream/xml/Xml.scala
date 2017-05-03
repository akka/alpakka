/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package com.drewhk.stream.xml

import akka.NotUsed
import akka.stream.scaladsl.Flow
import akka.stream.{ Attributes, FlowShape, Inlet, Outlet }
import akka.stream.stage.{ GraphStage, GraphStageLogic, InHandler, OutHandler }
import akka.util.ByteString
import com.fasterxml.aalto.{ AsyncByteArrayFeeder, AsyncXMLInputFactory, AsyncXMLStreamReader }
import com.fasterxml.aalto.stax.InputFactoryImpl

import scala.annotation.tailrec
import scala.collection.immutable

object Xml {

  /**
   * XML parsing events emitted by the parser flow. These roughly correspond to Java XMLEvent types.
   */
  sealed trait ParseEvent
  sealed trait TextEvent extends ParseEvent {
    def text: String
  }

  case object StartDocument extends ParseEvent
  case object EndDocument extends ParseEvent
  final case class StartElement(localName: String, attributes: Map[String, String]) extends ParseEvent
  final case class EndElement(localName: String) extends ParseEvent
  final case class Characters(text: String) extends TextEvent
  final case class ProcessingInstruction(target: Option[String], data: Option[String]) extends ParseEvent
  final case class Comment(text: String) extends ParseEvent
  final case class CData(text: String) extends TextEvent

  /**
   * Parser Flow that takes a stream of ByteStrings and parses them to XML events similar to SAX.
   */
  val parser: Flow[ByteString, ParseEvent, NotUsed] =
    Flow.fromGraph(new StreamingXmlParser)

  /**
   * A Flow that transforms a stream of XML ParseEvents. This stage coalesces consequitive CData and Characters
   * events into a single Characters event or fails if the buffered string is larger than the maximum defined.
   */
  def coalesce(maximumTextLength: Int): Flow[ParseEvent, ParseEvent, NotUsed] =
    Flow.fromGraph(new Coalesce(maximumTextLength))

  /**
   * A Flow that transforms a stream of XML ParseEvents. This stage filters out any event not corresponding to
   * a certain path in the XML document. Any event that is under the specified path (including subpaths) is passed
   * through.
   */
  def subslice(path: immutable.Seq[String]): Flow[ParseEvent, ParseEvent, NotUsed] =
    Flow.fromGraph(new Subslice(path))

  private class StreamingXmlParser extends GraphStage[FlowShape[ByteString, ParseEvent]] {
    val in: Inlet[ByteString] = Inlet("XMLParser.in")
    val out: Outlet[ParseEvent] = Outlet("XMLParser.out")
    override val shape: FlowShape[ByteString, ParseEvent] = FlowShape(in, out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) with InHandler with OutHandler {

        import javax.xml.stream.XMLStreamConstants

        private val feeder: AsyncXMLInputFactory = new InputFactoryImpl()
        private val parser: AsyncXMLStreamReader[AsyncByteArrayFeeder] = feeder.createAsyncFor(Array.empty)

        setHandlers(in, out, this)

        override def onPush(): Unit = {
          val array = grab(in).toArray
          parser.getInputFeeder.feedInput(array, 0, array.length)
          advanceParser()
        }

        override def onPull(): Unit = advanceParser()

        override def onUpstreamFinish(): Unit = {
          parser.getInputFeeder.endOfInput()
          if (!parser.hasNext) completeStage()
          else if (isAvailable(out)) advanceParser()
        }

        @tailrec private def advanceParser(): Unit = {
          if (parser.hasNext) {
            parser.next() match {
              case AsyncXMLStreamReader.EVENT_INCOMPLETE =>
                if (!isClosed(in)) pull(in)
                else failStage(new IllegalStateException("Stream finished before event was fully parsed."))

              case XMLStreamConstants.START_DOCUMENT =>
                push(out, StartDocument)

              case XMLStreamConstants.END_DOCUMENT =>
                push(out, EndDocument)
                completeStage()

              case XMLStreamConstants.START_ELEMENT =>
                val attributes = (0 until parser.getAttributeCount).map { i =>
                  parser.getAttributeLocalName(i) -> parser.getAttributeValue(i)
                }.toMap

                push(out, StartElement(parser.getLocalName, attributes))

              case XMLStreamConstants.END_ELEMENT =>
                push(out, EndElement(parser.getLocalName))

              case XMLStreamConstants.CHARACTERS =>
                push(out, Characters(parser.getText))

              case XMLStreamConstants.PROCESSING_INSTRUCTION =>
                push(out, ProcessingInstruction(Option(parser.getPITarget), Option(parser.getPIData)))

              case XMLStreamConstants.COMMENT =>
                push(out, Comment(parser.getText))

              case XMLStreamConstants.CDATA =>
                push(out, CData(parser.getText))

              // Do not support DTD, SPACE, NAMESPACE, NOTATION_DECLARATION, ENTITY_DECLARATION, PROCESSING_INSTRUCTION
              // ATTRIBUTE is handled in START_ELEMENT implicitly

              case x =>
                if (parser.hasNext) advanceParser()
                else completeStage()
            }
          } else completeStage()
        }
      }
  }

  private class Coalesce(maximumTextLength: Int) extends GraphStage[FlowShape[ParseEvent, ParseEvent]] {
    val in: Inlet[ParseEvent] = Inlet("XMLCoalesce.in")
    val out: Outlet[ParseEvent] = Outlet("XMLCoalesce.out")
    override val shape: FlowShape[ParseEvent, ParseEvent] = FlowShape(in, out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) with InHandler with OutHandler {
        private var isBuffering = false
        private val buffer = new StringBuilder

        override def onPull(): Unit = pull(in)

        override def onPush(): Unit = grab(in) match {
          case t: TextEvent =>
            if (t.text.length + buffer.length > maximumTextLength)
              failStage(new IllegalStateException(s"Too long character sequence, maximum is $maximumTextLength but got " +
                s"${t.text.length + buffer.length - maximumTextLength} more "))
            else {
              buffer.append(t.text)
              isBuffering = true
              pull(in)
            }
          case other =>
            if (isBuffering) {
              val coalesced = buffer.toString()
              isBuffering = false
              buffer.clear()
              emit(out, Characters(coalesced),
                () => emit(out, other,
                  () => if (isClosed(in)) completeStage()))
            } else {
              push(out, other)
            }
        }

        override def onUpstreamFinish(): Unit = {
          if (isBuffering) emit(out, Characters(buffer.toString()), () => completeStage())
          else completeStage()
        }

        setHandlers(in, out, this)
      }
  }

  private class Subslice(path: immutable.Seq[String]) extends GraphStage[FlowShape[ParseEvent, ParseEvent]] {
    val in: Inlet[ParseEvent] = Inlet("XMLSubslice.in")
    val out: Outlet[ParseEvent] = Outlet("XMLSubslice.out")
    override val shape: FlowShape[ParseEvent, ParseEvent] = FlowShape(in, out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) with OutHandler {
        private var expected = path.toList
        private var matchedSoFar: List[String] = Nil

        override def onPull(): Unit = pull(in)

        if (path.isEmpty) setHandler(in, passThrough) else setHandler(in, partialMatch)
        setHandler(out, this)

        val passThrough: InHandler = new InHandler {
          var depth = 0

          override def onPush(): Unit = grab(in) match {
            case start: StartElement =>
              depth += 1
              push(out, start)
            case end: EndElement =>
              if (depth == 0) {
                expected = matchedSoFar.head :: Nil
                matchedSoFar = matchedSoFar.tail
                setHandler(in, partialMatch)
                pull(in)
              } else {
                depth -= 1
                push(out, end)
              }
            case other =>
              push(out, other)
          }
        }

        lazy val partialMatch: InHandler = new InHandler {

          override def onPush(): Unit = grab(in) match {
            case StartElement(name, _) =>
              if (name == expected.head) {
                matchedSoFar = expected.head :: matchedSoFar
                expected = expected.tail
                if (expected.isEmpty) {
                  setHandler(in, passThrough)
                }
              } else {
                setHandler(in, noMatch)
              }
              pull(in)
            case EndElement(name) =>
              expected = matchedSoFar.head :: expected
              matchedSoFar = matchedSoFar.tail
              pull(in)
            case other =>
              pull(in)
          }

        }

        lazy val noMatch: InHandler = new InHandler {
          var depth = 0

          override def onPush(): Unit = grab(in) match {
            case start: StartElement =>
              depth += 1
              pull(in)
            case end: EndElement =>
              if (depth == 0) setHandler(in, partialMatch)
              else depth -= 1
              pull(in)
            case other =>
              pull(in)
          }
        }

      }
  }

}
