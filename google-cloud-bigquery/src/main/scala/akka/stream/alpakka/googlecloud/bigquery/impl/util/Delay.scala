/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.impl.util

import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.annotation.InternalApi
import akka.stream.alpakka.googlecloud.bigquery.impl.util.DelayFlow.DelayStrategy
import akka.stream.scaladsl.{GraphDSL, Merge}
import akka.stream.{FlowShape, Graph}

import scala.concurrent.duration.FiniteDuration

@InternalApi
private[impl] object Delay {
  def apply[T](
      shouldDelay: T => Boolean,
      maxDelay: Int,
      delayUnit: TimeUnit = TimeUnit.SECONDS
  ): Graph[FlowShape[T, T], NotUsed] =
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      val splitter = builder.add(Splitter[T](shouldDelay)())
      val delayFlow =
        builder.add(DelayFlow[T](() => new FibonacciStrategy[T](delayUnit, maxDelay)))
      val merge = builder.add(Merge[T](2, eagerComplete = true))

      splitter.out(0) ~> delayFlow
      delayFlow ~> merge.in(0)

      splitter.out(1) ~> merge.in(1)

      new FlowShape(splitter.in, merge.out)
    }

  private final class FibonacciStrategy[T](delayUnit: TimeUnit, maxDelay: Int) extends DelayStrategy[T] {
    val fibs: Stream[Int] = 1 #:: 1 #:: (fibs zip fibs.tail).map { case (a, b) => a + b }
    var idx = 0
    override def nextDelay(elem: T): FiniteDuration = {
      val delay = fibs(idx)

      idx += 1

      if (delay > maxDelay) {
        throw new IllegalStateException(s"Maximum delay ($maxDelay) exceeded: ${FiniteDuration(delay, delayUnit)}")
      }

      FiniteDuration(delay, delayUnit)
    }
  }
}
