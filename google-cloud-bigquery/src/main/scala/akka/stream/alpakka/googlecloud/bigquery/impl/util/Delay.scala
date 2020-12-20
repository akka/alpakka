/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.impl.util

import akka.NotUsed
import akka.annotation.InternalApi
import akka.pattern.BackoffSupervisor
import akka.stream.alpakka.googlecloud.bigquery.RetrySettings
import akka.stream.scaladsl.{DelayStrategy, Flow, GraphDSL, Merge, Partition}
import akka.stream.{DelayOverflowStrategy, FlowShape, Graph}

import scala.concurrent.duration.FiniteDuration

@InternalApi
private[impl] object Delay {

  def apply[T](retrySettings: RetrySettings)(shouldDelay: T => Boolean): Graph[FlowShape[T, T], NotUsed] =
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      val partition = builder.add(Partition[T](2, t => if (shouldDelay(t)) 0 else 1))
      val delay = builder.add(delayFlow[T](retrySettings))
      val merge = builder.add(Merge[T](2))

      partition.out(0) ~> delay ~> merge.in(0)
      partition.out(1) ~> merge.in(1)

      new FlowShape(partition.in, merge.out)
    }

  private def delayFlow[T](settings: RetrySettings): Flow[T, T, NotUsed] = {
    Flow[T].delayWith(() => backoffDelayStrategy(settings), DelayOverflowStrategy.backpressure)
  }

  private def backoffDelayStrategy[T](settings: RetrySettings): DelayStrategy[T] = new DelayStrategy[T] {
    private var retryCount = 0
    override def nextDelay(elem: T): FiniteDuration = {
      import settings._
      val delay = BackoffSupervisor.calculateDelay(retryCount, minBackoff, maxBackoff, randomFactor)
      retryCount += 1
      delay
    }
  }
}
