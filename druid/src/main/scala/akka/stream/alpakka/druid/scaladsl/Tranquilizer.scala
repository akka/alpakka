/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.druid.scaladsl

import akka.{Done, NotUsed}
import akka.stream.ActorAttributes
import akka.stream.alpakka.druid.TranquilizerSettings
import akka.stream.alpakka.druid.internal.TranquilizerStage
import akka.stream.scaladsl.{Flow, Keep, Sink}

import scala.concurrent.Future

object Tranquilizer {

  def sink[A](config: TranquilizerSettings[A]): Sink[A, Future[Done]] =
    Flow[A].via(flow(config)).toMat(Sink.ignore)(Keep.right)

  def flow[A](settings: TranquilizerSettings[A]): Flow[A, A, NotUsed] = {
    val flow = Flow.fromGraph(new TranquilizerStage[A](settings)).mapAsync(settings.parallelism)(identity)
    if (settings.dispatcher.isEmpty) flow
    else flow.withAttributes(ActorAttributes.dispatcher(settings.dispatcher))
  }

}
