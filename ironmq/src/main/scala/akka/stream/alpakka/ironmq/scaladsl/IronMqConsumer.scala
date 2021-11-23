/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.ironmq.scaladsl

import akka.NotUsed
import akka.dispatch.ExecutionContexts
import akka.stream.alpakka.ironmq._
import akka.stream.alpakka.ironmq.impl.IronMqPullStage
import akka.stream.scaladsl._

object IronMqConsumer {

  def atMostOnceSource(queueName: String, settings: IronMqSettings): Source[Message, NotUsed] =
    Source.fromGraph(new IronMqPullStage(queueName, settings)).mapAsync(1) { cm =>
      cm.commit().map(_ => cm.message)(ExecutionContexts.parasitic)
    }

  def atLeastOnceSource[K, V](queueName: String, settings: IronMqSettings): Source[CommittableMessage, NotUsed] =
    Source.fromGraph(new IronMqPullStage(queueName, settings))

}
