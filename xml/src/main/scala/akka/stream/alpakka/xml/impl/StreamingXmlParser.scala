/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.xml.impl
import akka.annotation.InternalApi
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.alpakka.xml._
import akka.stream.alpakka.xml.impl.StreamingXmlParser.withStreamingFinishedException
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.util.ByteString
import com.fasterxml.aalto.{AsyncByteArrayFeeder, AsyncXMLInputFactory, AsyncXMLStreamReader}
import com.fasterxml.aalto.stax.InputFactoryImpl
import com.fasterxml.aalto.util.IllegalCharHandler.ReplacingIllegalCharHandler

import scala.annotation.tailrec

private[xml] object StreamingXmlParser {
  lazy val withStreamingFinishedException = new IllegalStateException("Stream finished before event was fully parsed.")
}

/**
 * INTERNAL API
 */
@InternalApi private[xml] class StreamingXmlParser[A, B, Ctx](ignoreInvalidChars: Boolean,
                                                              configureFactory: AsyncXMLInputFactory => Unit,
                                                              getByteString: A => ByteString,
                                                              getContext: A => Ctx,
                                                              buildOutput: (ParseEvent, Ctx) => B)
    extends GraphStage[FlowShape[A, B]] {
  val in: Inlet[A] = Inlet("XMLParser.in")
  val out: Outlet[B] = Outlet("XMLParser.out")
  override val shape: FlowShape[A, B] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with InHandler with OutHandler {
      private var started: Boolean = false
      private var context: Ctx = _

      import javax.xml.stream.XMLStreamConstants

      private val factory: AsyncXMLInputFactory = new InputFactoryImpl()
      configureFactory(factory)
      private val parser: AsyncXMLStreamReader[AsyncByteArrayFeeder] = factory.createAsyncForByteArray()
      if (ignoreInvalidChars) {
        parser.getConfig.setIllegalCharHandler(new ReplacingIllegalCharHandler(0))
      }

      setHandlers(in, out, this)

      override def onPush(): Unit = {
        val a = grab(in)
        val bs = getByteString(a)
        context = getContext(a)
        val array = bs.toArray
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
            case AsyncXMLStreamReader.EVENT_INCOMPLETE if isClosed(in) && !started => completeStage()
            case AsyncXMLStreamReader.EVENT_INCOMPLETE if isClosed(in) => failStage(withStreamingFinishedException)
            case AsyncXMLStreamReader.EVENT_INCOMPLETE => pull(in)

            case XMLStreamConstants.START_DOCUMENT =>
              started = true
              push(out, buildOutput(StartDocument, context))

            case XMLStreamConstants.END_DOCUMENT =>
              push(out, buildOutput(EndDocument, context))
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
                buildOutput(StartElement(parser.getLocalName,
                                         attributes,
                                         optPrefix.filterNot(_ == ""),
                                         optNs.filterNot(_ == ""),
                                         namespaceCtx = namespaces),
                            context)
              )

            case XMLStreamConstants.END_ELEMENT =>
              push(out, buildOutput(EndElement(parser.getLocalName), context))

            case XMLStreamConstants.CHARACTERS =>
              push(out, buildOutput(Characters(parser.getText), context))

            case XMLStreamConstants.PROCESSING_INSTRUCTION =>
              push(out,
                   buildOutput(ProcessingInstruction(Option(parser.getPITarget), Option(parser.getPIData)), context))

            case XMLStreamConstants.COMMENT =>
              push(out, buildOutput(Comment(parser.getText), context))

            case XMLStreamConstants.CDATA =>
              push(out, buildOutput(CData(parser.getText), context))

            // Do not support DTD, SPACE, NAMESPACE, NOTATION_DECLARATION, ENTITY_DECLARATION, PROCESSING_INSTRUCTION
            // ATTRIBUTE is handled in START_ELEMENT implicitly

            case x =>
              advanceParser()
          }
        } else completeStage()
    }
}
