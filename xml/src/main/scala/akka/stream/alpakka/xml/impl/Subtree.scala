/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.xml.impl
import akka.annotation.InternalApi
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.alpakka.xml._
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import javax.xml.parsers.DocumentBuilderFactory
import org.w3c.dom.Element

import scala.collection.immutable

/**
 * INTERNAL API
 */
@InternalApi private[xml] class Subtree(path: immutable.Seq[String])
    extends GraphStage[FlowShape[ParseEvent, Element]] {
  val in: Inlet[ParseEvent] = Inlet("XMLSubtree.in")
  val out: Outlet[Element] = Outlet("XMLSubtree.out")
  override val shape: FlowShape[ParseEvent, Element] = FlowShape(in, out)

  private val doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument()

  private def createElement(start: StartElement): Element = {
    val element = start.namespace match {
      case Some(ns) => doc.createElementNS(ns, start.localName)
      case None => doc.createElement(start.localName)
    }
    start.attributes.foreach {
      case (name, value) =>
        element.setAttribute(name, value)
    }
    element
  }

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with OutHandler {
      private var expected = path.toList
      private var matchedSoFar: List[String] = Nil
      private var elementStack: List[Element] = Nil

      override def onPull(): Unit = pull(in)

      val matching: InHandler = new InHandler {

        override def onPush(): Unit = grab(in) match {
          case start: StartElement =>
            val element = createElement(start)
            elementStack.headOption.foreach { head =>
              head.appendChild(element)
            }
            elementStack = element :: elementStack
            pull(in)
          case _: EndElement =>
            elementStack match {
              case head :: Nil =>
                expected = matchedSoFar.head :: Nil
                matchedSoFar = matchedSoFar.tail
                push(out, head)
                elementStack = Nil
                setHandler(in, partialMatch)
              case _ =>
                elementStack = elementStack.tail
                pull(in)
            }
          case cdata: CData =>
            elementStack.headOption.foreach { element =>
              element.appendChild(doc.createCDATASection(cdata.text))
            }
            pull(in)
          case text: TextEvent =>
            elementStack.headOption.foreach { element =>
              element.appendChild(doc.createTextNode(text.text))
            }
            pull(in)
          case other =>
            pull(in)
        }
      }

      if (path.isEmpty) setHandler(in, matching) else setHandler(in, partialMatch)
      setHandler(out, this)

      lazy val partialMatch: InHandler = new InHandler {

        override def onPush(): Unit = grab(in) match {
          case start: StartElement =>
            if (start.localName == expected.head) {
              matchedSoFar = expected.head :: matchedSoFar
              expected = expected.tail
              if (expected.isEmpty) {
                val element = createElement(start)
                elementStack = element :: Nil
                setHandler(in, matching)
              }
            } else {
              setHandler(in, noMatch)
            }
            pull(in)
          case _: EndElement =>
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
          case _: StartElement =>
            depth += 1
            pull(in)
          case _: EndElement =>
            if (depth == 0) setHandler(in, partialMatch)
            else depth -= 1
            pull(in)
          case other =>
            pull(in)
        }
      }

    }
}
