/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.s3.impl

import akka.actor.ActorSystem
import akka.stream._
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
      }, () => throw new Error("No element received"))

  private def extractInterpreter(gi: GraphInterpreter) = gi match {
    case null => throw new Error("GraphInterpreter not set")
    case gi => gi
  }

  private def extractMat(gi: GraphInterpreter) =
    extractInterpreter(gi).materializer match {
      case am: ActorMaterializer => am
      case _ => throw new Error("ActorMaterializer required")
    }

  private[impl] def extractAttributes(gi: GraphInterpreter) =
    extractInterpreter(gi).activeStage.attributes
}

object ActorSystemAccess {
  def source[T, M](factory: ActorSystem => Source[T, M]): Source[T, Future[M]] =
    MaterializerAccess.source(am => factory(am.system))
}

object AttributesAccess {
  def source[T, M](factory: Attributes => Source[T, M]): Source[T, Future[M]] =
    Source
      .lazily(() => {
        factory(MaterializerAccess.extractAttributes(GraphInterpreter.currentInterpreterOrNull))
      })

  def flow[T, U, M](factory: Attributes => Flow[T, U, M]): Flow[T, U, Future[Option[M]]] =
    Flow
      .lazyInitAsync(() => {
        Future.successful(factory(MaterializerAccess.extractAttributes(GraphInterpreter.currentInterpreterOrNull)))
      })

  def sink[T, M](factory: Attributes => Sink[T, M]): Sink[T, Future[M]] =
    Sink
      .lazyInit(
        _ => {
          Future.successful(factory(MaterializerAccess.extractAttributes(GraphInterpreter.currentInterpreterOrNull)))
        },
        () => throw new Error("No element received")
      )
}
