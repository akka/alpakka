/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs.impl

import akka.annotation.InternalApi
import akka.stream.scaladsl.{Flow, GraphDSL, Merge, Partition}
import akka.stream.{DelayOverflowStrategy, FlowShape}

import scala.concurrent.duration.FiniteDuration

@InternalApi
private[akka] class ControlledThrottling[T](delay: FiniteDuration) {
  private var isThrottling: Boolean = false

  def setThrottling(shouldBeThrottled: Boolean): Unit = {
    isThrottling = shouldBeThrottled
  }

  def apply(): Flow[T, T, _] = {
    Flow.fromGraph(GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      val partition = b.add(Partition[T](2, _ => if (isThrottling) 1 else 0 ))
      val throttle = b.add(Flow[T].delay(delay, DelayOverflowStrategy.backpressure).throttle(1, delay))
      val merge = b.add(Merge[T](2))

      partition       ~>       merge
      partition ~> throttle ~> merge

      FlowShape(partition.in, merge.out)
    })
  }
}
