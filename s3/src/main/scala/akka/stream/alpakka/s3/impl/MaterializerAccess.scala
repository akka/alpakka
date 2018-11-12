/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.s3.impl

import akka.stream._
import akka.stream.impl.fusing.GraphInterpreter
import akka.stream.scaladsl.{Flow, Sink, Source}

import scala.concurrent.Future

object MaterializerAccess {
  def source[T, M](factory: ActorMaterializer => Source[T, M]): Source[T, M] =
    Source
      .lazily(() => {
        factory(extractMat(GraphInterpreter.currentInterpreterOrNull))
      })
      // FIXME propagate original mat value
      .mapMaterializedValue(_.asInstanceOf[M])

  def flow[T, U, M](factory: ActorMaterializer => Flow[T, U, M]): Flow[T, U, M] =
    Flow
      .lazyInitAsync(() => {
        Future.successful(factory(extractMat(GraphInterpreter.currentInterpreterOrNull)))
      })
      // FIXME propagate original mat value
      .mapMaterializedValue(_.asInstanceOf[M])

  def sink[T, M](factory: ActorMaterializer => Sink[T, M]): Sink[T, M] =
    Sink
      .lazyInitAsync(() => {
        Future.successful(factory(extractMat(GraphInterpreter.currentInterpreterOrNull)))
      })
      // FIXME propagate original mat value
      .mapMaterializedValue(_.asInstanceOf[M])

  private def extractMat(gi: GraphInterpreter) = gi match {
    case null => throw new Error("GraphInterpreter not set")
    case gi => gi.materializer match {
      case am: ActorMaterializer => am
      case _ => throw new Error("ActorMaterializer required")
    }
  }
}
