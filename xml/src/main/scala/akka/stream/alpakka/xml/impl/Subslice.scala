/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.xml.impl
import akka.annotation.InternalApi
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.alpakka.xml.{EndElement, ParseEvent, StartElement}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}

import scala.collection.immutable

/**
 * INTERNAL API
 */
@InternalApi private[xml] class Subslice(path: immutable.Seq[String])
    extends GraphStage[FlowShape[ParseEvent, ParseEvent]] {
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
