/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.reference.impl

import akka.Done
import akka.annotation.InternalApi
import akka.event.Logging
import akka.stream._
import akka.stream.alpakka.reference.{ReferenceReadResult, SourceSettings}
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, OutHandler}
import akka.util.ByteString

import scala.collection.immutable
import scala.concurrent.{Future, Promise}
import scala.util.Success

/**
 * INTERNAL API
 *
 * Private package hides the class from the API in Scala. However it is still
 * visible in Java. Use "InternalApi" annotation and "INTERNAL API" as the first
 * line in scaladoc to communicate to Java developers that this is private API.
 */
@InternalApi private[reference] final class ReferenceSourceStageLogic(
    val settings: SourceSettings,
    val startupPromise: Promise[Done],
    val shape: SourceShape[ReferenceReadResult]
) extends GraphStageLogic(shape) {

  private def out = shape.out

  /**
   * Initialization logic
   */
  override def preStart(): Unit =
    startupPromise.success(Done)

  setHandler(out, new OutHandler {
    override def onPull(): Unit = push(
      out,
      new ReferenceReadResult(immutable.Seq(ByteString("one")), Success(100))
    )
  })

  /**
   * Cleanup logic
   */
  override def postStop(): Unit = {}
}

/**
 * INTERNAL API
 */
@InternalApi private[reference] final class ReferenceSourceStage(settings: SourceSettings)
    extends GraphStageWithMaterializedValue[SourceShape[ReferenceReadResult], Future[Done]] {
  val out: Outlet[ReferenceReadResult] = Outlet(Logging.simpleName(this) + ".out")

  override def initialAttributes: Attributes =
    Attributes.name(Logging.simpleName(this))

  override val shape: SourceShape[ReferenceReadResult] = SourceShape(out)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) = {
    // materialized value created as a new instance on every materialization
    val startupPromise = Promise[Done]
    val logic = new ReferenceSourceStageLogic(settings, startupPromise, shape)
    (logic, startupPromise.future)
  }

}
