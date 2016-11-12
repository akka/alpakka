/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.caffeine.scaladsl

import akka.stream.alpakka.caffeine.Aggregator
import akka.stream.alpakka.caffeine.internal.{CaffeineFanOut2Stage, CaffeineFlowStage}

object CaffeineFlow {
  def apply[K, V, R](aggregator: Aggregator[K, V, R]) = new CaffeineFlowStage(aggregator)
  def tee[K, V, R](aggregator: Aggregator[K, V, R]) = new CaffeineFanOut2Stage(aggregator)

}
