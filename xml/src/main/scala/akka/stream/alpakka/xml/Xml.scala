/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
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
import scala.collection.immutable.DefaultMap
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

private class MapOverTraversable[A, K, V](source: Traversable[A], fKey: A => K, fValue: A => V)
    extends DefaultMap[K, V] {
  override def get(key: K): Option[V] = source.find(a => fKey(a) == key).map(fValue)

  override def iterator: Iterator[(K, V)] = source.toIterator.map(a => (fKey(a), fValue(a)))

}

final case class Namespace(uri: String, prefix: Option[String] = None)

object Namespace {

  /**
   * Java API
   */
  def create(uri: String, prefix: Optional[String]) =
    Namespace(uri, prefix.asScala)

}

final case class Attribute(name: String, value: String, prefix: Option[String] = None, namespace: Option[String] = None)
object Attribute {

  /**
   * Java API
   */
  def create(name: String, value: String, prefix: Optional[String], namespace: Optional[String]) =
    Attribute(name, value, prefix.asScala, namespace.asScala)

  /**
   * Java API
   */
  def create(name: String, value: String) = Attribute(name, value)
}

final case class StartElement(localName: String,
                              attributesList: List[Attribute] = List.empty[Attribute],
                              prefix: Option[String] = None,
                              namespace: Option[String] = None,
                              namespaceCtx: List[Namespace] = List.empty[Namespace])
    extends ParseEvent {
  val attributes: Map[String, String] =
    new MapOverTraversable[Attribute, String, String](attributesList, _.name, _.value)

  def findAttribute(name: String): Option[Attribute] = attributesList.find(_.name == name)
}

object StartElement {

  def fromMapToAttributeList(prefix: Option[String] = None,
                             namespace: Option[String] = None)(attributes: Map[String, String]): List[Attribute] =
    attributes.toList.map {
      case (name, value) => Attribute(name, value, prefix, namespace)
    }

  def apply(localName: String, attributes: Map[String, String]): StartElement = {
    val attributesList = fromMapToAttributeList()(attributes)
    new StartElement(localName, attributesList, prefix = None, namespace = None, namespaceCtx = List.empty[Namespace])
  }

  /**
   * Java API
   */
  def create(localName: String,
             attributesList: java.util.List[Attribute],
             prefix: Optional[String],
             namespace: Optional[String],
             namespaceCtx: java.util.List[Namespace]): StartElement =
    new StartElement(localName,
                     attributesList.asScala.toList,
                     prefix.asScala,
                     namespace.asScala,
                     namespaceCtx.asScala.toList)

  /**
   * Java API
   */
  def create(localName: String,
             attributesList: java.util.List[Attribute],
             prefix: Optional[String],
             namespace: Optional[String]): StartElement =
    new StartElement(localName, attributesList.asScala.toList, prefix.asScala, namespace.asScala, List.empty[Namespace])

  /**
   * Java API
   */
  def create(localName: String, attributesList: java.util.List[Attribute], namespace: String): StartElement =
    new StartElement(localName,
                     attributesList.asScala.toList,
                     prefix = None,
                     namespace = Some(namespace),
                     namespaceCtx = List(Namespace(namespace)))

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
                  val optNs = Option(parser.getAttributeNamespace(i)).filterNot(_ == "")
                  val optPrefix = Option(parser.getAttributePrefix(i)).filterNot(_ == "")
                  Attribute(name = parser.getAttributeLocalName(i),
                            value = parser.getAttributeValue(i),
                            prefix = optPrefix,
                            namespace = optNs)
                }.toList
                val namespaces = (0 until parser.getNamespaceCount).map { i =>
                  val namespace = parser.getNamespaceURI(i)
                  val optPrefix = Option(parser.getNamespacePrefix(i)).filterNot(_ == "")
                  Namespace(namespace, optPrefix)
                }.toList
                val optPrefix = Option(parser.getPrefix)
                val optNs = optPrefix.flatMap(prefix => Option(parser.getNamespaceURI(prefix)))
                push(
                  out,
                  StartElement(parser.getLocalName,
                               attributes,
                               optPrefix.filterNot(_ == ""),
                               optNs.filterNot(_ == ""),
                               namespaceCtx = namespaces)
                )

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
                advanceParser()
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

        def writeAttributes(attributes: List[Attribute]): Unit =
          attributes.foreach { att =>
            att match {
              case Attribute(name, value, Some(prefix), Some(namespace)) =>
                output.writeAttribute(prefix, namespace, name, value)
              case Attribute(name, value, None, Some(namespace)) =>
                output.writeAttribute(namespace, name, value)
              case Attribute(name, value, Some(_), None) =>
                output.writeAttribute(name, value)
              case Attribute(name, value, None, None) =>
                output.writeAttribute(name, value)
            }

          }

        override def onPush(): Unit = {
          val ev: ParseEvent = grab(in)
          ev match {
            case StartDocument =>
              output.writeStartDocument()

            case EndDocument =>
              output.writeEndDocument()

            case StartElement(localName, attributes, optPrefix, Some(namespace), namespaceCtx) =>
              val prefix = optPrefix.getOrElse("")
              output.setPrefix(prefix, namespace)
              output.writeStartElement(prefix, localName, namespace)
              namespaceCtx.foreach(ns => output.writeNamespace(ns.prefix.getOrElse(""), ns.uri))
              writeAttributes(attributes)

            case StartElement(localName, attributes, Some(_), None, namespaceCtx) => // Shouldn't happened
              output.writeStartElement(localName)
              namespaceCtx.foreach(ns => output.writeNamespace(ns.prefix.getOrElse(""), ns.uri))
              writeAttributes(attributes)

            case StartElement(localName, attributes, None, None, namespaceCtx) =>
              output.writeStartElement(localName)
              namespaceCtx.foreach(ns => output.writeNamespace(ns.prefix.getOrElse(""), ns.uri))
              writeAttributes(attributes)

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
            case StartElement(name, _, _, _, _) =>
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
