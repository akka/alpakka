/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.xml

import java.nio.charset.Charset
import java.util.Optional
import javax.xml.stream.XMLOutputFactory

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.util.{ByteString, ByteStringBuilder}
import com.fasterxml.aalto.stax.InputFactoryImpl
import com.fasterxml.aalto.{AsyncByteArrayFeeder, AsyncXMLInputFactory, AsyncXMLStreamReader}

import scala.annotation.tailrec
import scala.collection.immutable
import scala.collection.JavaConverters._
import scala.compat.java8.OptionConverters._

/**
 * XML parsing events emitted by the parser flow. These roughly correspond to Java XMLEvent types.
 */
sealed trait ParseEvent
sealed trait TextEvent extends ParseEvent {
  def text: String
}

case object StartDocument extends ParseEvent {

  /**
   * Java API
   */
  def getInstance(): StartDocument.type = this
}

case object EndDocument extends ParseEvent {

  /**
   * Java API
   */
  def getInstance(): EndDocument.type = this
}
final case class StartElement(localName: String, attributes: Map[String, String]) extends ParseEvent
object StartElement {

  /**
   * Java API
   */
  def create(localName: String, attributes: java.util.Map[String, String]): StartElement =
    StartElement(localName, attributes.asScala.toMap)
}
final case class EndElement(localName: String) extends ParseEvent
object EndElement {

  /**
   * Java API
   */
  def create(localName: String) =
    EndElement(localName)
}
final case class Characters(text: String) extends TextEvent
object Characters {

  /**
   * Java API
   */
  def create(text: String) =
    Characters(text)
}
final case class ProcessingInstruction(target: Option[String], data: Option[String]) extends ParseEvent
object ProcessingInstruction {

  /**
   * Java API
   */
  def create(target: Optional[String], data: Optional[String]) =
    ProcessingInstruction(target.asScala, data.asScala)
}
final case class Comment(text: String) extends ParseEvent
object Comment {

  /**
   * Java API
   */
  def create(text: String) =
    Comment(text)
}
final case class CData(text: String) extends TextEvent
object CData {

  /**
   * Java API
   */
  def create(text: String) =
    CData(text)
}

object Xml {

  /**
   * Internal API
   */
  private[xml] class StreamingXmlParser extends GraphStage[FlowShape[ByteString, ParseEvent]] {
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

        @tailrec private def advanceParser(): Unit =
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

  /**
   * Internal API
   */
  private[xml] class StreamingXmlWriter(charset: Charset) extends GraphStage[FlowShape[ParseEvent, ByteString]] {
    val in: Inlet[ParseEvent] = Inlet("XMLWriter.in")
    val out: Outlet[ByteString] = Outlet("XMLWriter.out")
    override val shape: FlowShape[ParseEvent, ByteString] = FlowShape(in, out)

    private val xMLOutputFactory = XMLOutputFactory.newInstance()

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) with InHandler with OutHandler {
        val byteStringBuilder = new ByteStringBuilder()

        val output = xMLOutputFactory.createXMLStreamWriter(byteStringBuilder.asOutputStream, charset.name())

        setHandlers(in, out, this)

        override def onPush(): Unit = {
          val ev: ParseEvent = grab(in)
          ev match {
            case StartDocument =>
              output.writeStartDocument()

            case EndDocument =>
              output.writeEndDocument()

            case StartElement(localName, attributes) =>
              output.writeStartElement(localName)
              attributes.foreach(t => output.writeAttribute(t._1, t._2))

            case EndElement(_) =>
              output.writeEndElement()

            case Characters(text) =>
              output.writeCharacters(text)
            case ProcessingInstruction(Some(target), Some(data)) =>
              output.writeProcessingInstruction(target, data)

            case ProcessingInstruction(Some(target), None) =>
              output.writeProcessingInstruction(target)

            case ProcessingInstruction(None, Some(data)) =>
              output.writeProcessingInstruction(None.orNull, data)
            case ProcessingInstruction(None, None) =>
            case Comment(text) =>
              output.writeComment(text)

            case CData(text) =>
              output.writeCData(text)
          }
          push(out, byteStringBuilder.result().compact)
          byteStringBuilder.clear()
        }

        override def onPull(): Unit = pull(in)
      }
  }

  /**
   * Internal API
   */
  private[xml] class Coalesce(maximumTextLength: Int) extends GraphStage[FlowShape[ParseEvent, ParseEvent]] {
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
              failStage(
                new IllegalStateException(
                  s"Too long character sequence, maximum is $maximumTextLength but got " +
                  s"${t.text.length + buffer.length - maximumTextLength} more "
                )
              )
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
              emit(out, Characters(coalesced), () => emit(out, other, () => if (isClosed(in)) completeStage()))
            } else {
              push(out, other)
            }
        }

        override def onUpstreamFinish(): Unit =
          if (isBuffering) emit(out, Characters(buffer.toString()), () => completeStage())
          else completeStage()

        setHandlers(in, out, this)
      }
  }

  /**
   * Internal API
   */
  private[xml] class Subslice(path: immutable.Seq[String]) extends GraphStage[FlowShape[ParseEvent, ParseEvent]] {
    val in: Inlet[ParseEvent] = Inlet("XMLSubslice.in")
    val out: Outlet[ParseEvent] = Outlet("XMLSubslice.out")
    override val shape: FlowShape[ParseEvent, ParseEvent] = FlowShape(in, out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) with OutHandler {
        private var expected = path.toList
        private var matchedSoFar: List[String] = Nil

        override def onPull(): Unit = pull(in)

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

        if (path.isEmpty) setHandler(in, passThrough) else setHandler(in, partialMatch)
        setHandler(out, this)

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
