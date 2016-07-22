/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.contrib

import akka.japi.function
import akka.stream.stage.{ GraphStage, GraphStageLogic, InHandler, OutHandler }
import akka.stream.{ Attributes, FlowShape, Inlet, Outlet }

import scala.collection.immutable

object AccumulateWhileUnchanged {

  /**
   * Factory for [[AccumulateWhileUnchanged]] instances
   *
   * @param propertyExtractor a function to extract the observed element property
   * @tparam Element  type of accumulated elements
   * @tparam Property type of the observed property
   * @return [[AccumulateWhileUnchanged]] instance
   */
  def apply[Element, Property](propertyExtractor: Element => Property) = new AccumulateWhileUnchanged(propertyExtractor)

  /**
   * Java API: Factory for [[AccumulateWhileUnchanged]] instances
   *
   * @param propertyExtractor a function to extract the observed element property
   * @tparam Element  type of accumulated elements
   * @tparam Property type of the observed property
   * @return [[AccumulateWhileUnchanged]] instance
   */
  def create[Element, Property](propertyExtractor: function.Function[Element, Property]) = new AccumulateWhileUnchanged(propertyExtractor.apply)
}

/**
 * Accumulates elements of type [[Element]] while a property extracted with [[propertyExtractor]] remains unchanged,
 * emits an accumulated sequence when the property changes
 *
 * @param propertyExtractor a function to extract the observed element property
 * @tparam Element  type of accumulated elements
 * @tparam Property type of the observed property
 */
final class AccumulateWhileUnchanged[Element, Property](propertyExtractor: Element => Property) extends GraphStage[FlowShape[Element, immutable.Seq[Element]]] {

  val in = Inlet[Element]("AccumulateWhileUnchanged.in")
  val out = Outlet[immutable.Seq[Element]]("AccumulateWhileUnchanged.out")

  override def shape = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes) = new GraphStageLogic(shape) {

    private var currentState: Option[Property] = None
    private val buffer = Vector.newBuilder[Element]

    setHandlers(in, out, new InHandler with OutHandler {

      override def onPush(): Unit = {
        val nextElement = grab(in)
        val nextState = propertyExtractor(nextElement)

        if (currentState.isEmpty) currentState = Some(nextState)

        currentState match {
          case Some(`nextState`) =>
            buffer += nextElement
            pull(in)
          case _ =>
            val result = buffer.result()
            buffer.clear()
            buffer += nextElement
            push(out, result)
            currentState = Some(nextState)
        }
      }

      override def onPull(): Unit = {
        pull(in)
      }

      override def onUpstreamFinish(): Unit = {
        val result = buffer.result()
        if (result.nonEmpty) {
          emit(out, result)
        }
        completeStage()
      }
    })

    override def postStop(): Unit = {
      buffer.clear()
    }
  }
}
