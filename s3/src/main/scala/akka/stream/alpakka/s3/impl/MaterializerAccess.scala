/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.s3.impl

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.alpakka.s3.impl.MaterializerAccess.extractMat
import akka.stream.impl.fusing.GraphInterpreter
import akka.stream.scaladsl.{Flow, Sink, Source}

import scala.concurrent.Future

object MaterializerAccess {
  def source[T, M](factory: ActorMaterializer => Source[T, M]): Source[T, Future[M]] =
    Source
      .lazily(() => {
        factory(extractMat(GraphInterpreter.currentInterpreterOrNull))
      })

  def flow[T, U, M](factory: ActorMaterializer => Flow[T, U, M]): Flow[T, U, Future[Option[M]]] =
    Flow
      .lazyInitAsync(() => {
        Future.successful(factory(extractMat(GraphInterpreter.currentInterpreterOrNull)))
      })

  def sink[T, M](factory: ActorMaterializer => Sink[T, M]): Sink[T, Future[M]] =
    Sink
      .lazyInit(_ => {
        Future.successful(factory(extractMat(GraphInterpreter.currentInterpreterOrNull)))
      }, () => ???)

  private def extractMat(gi: GraphInterpreter) = gi match {
    case null => throw new Error("GraphInterpreter not set")
    case gi => gi.materializer match {
      case am: ActorMaterializer => am
      case _ => throw new Error("ActorMaterializer required")
    }
  }
}

object ActorSystemAccess {
  def source[T, M](factory: ActorSystem => Source[T, M]): Source[T, Future[M]] =
    MaterializerAccess.source(am => factory(am.system))
}
