/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.reference.impl

import akka.Done
import akka.annotation.InternalApi
import akka.event.Logging
import akka.stream._
import akka.stream.alpakka.reference.{ReferenceReadMessage, SourceSettings}
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
    val shape: SourceShape[ReferenceReadMessage]
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
      ReferenceReadMessage()
        .withData(immutable.Seq(ByteString("one")))
        .withBytesRead(Success(100))
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
@InternalApi private[reference] final class ReferenceSource(settings: SourceSettings)
    extends GraphStageWithMaterializedValue[SourceShape[ReferenceReadMessage], Future[Done]] {
  val out: Outlet[ReferenceReadMessage] = Outlet(Logging.simpleName(this) + ".out")

  override def initialAttributes: Attributes =
    Attributes.name(Logging.simpleName(this))

  override val shape: SourceShape[ReferenceReadMessage] = SourceShape(out)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) = {
    // materialized value created as a new instance on every materialization
    val startupPromise = Promise[Done]
    val logic = new ReferenceSourceStageLogic(settings, startupPromise, shape)
    (logic, startupPromise.future)
  }

}
