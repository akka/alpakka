/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.ironmq.scaladsl

import akka.NotUsed
import akka.dispatch.ExecutionContexts
import akka.stream.alpakka.ironmq._
import akka.stream.scaladsl._

object IronMqConsumer {

  def atMostOnceConsumerSource(queueName: Queue.Name, settings: IronMqSettings): Source[Message, NotUsed] =
    Source.fromGraph(new IronMqPullStage(queueName, settings)).mapAsync(1) { cm =>
      cm.commit().map(_ => cm.message)(ExecutionContexts.sameThreadExecutionContext)
    }

  def atLeastOnceConsumerSource[K, V](queueName: Queue.Name,
                                      settings: IronMqSettings): Source[CommittableMessage, NotUsed] =
    Source.fromGraph(new IronMqPullStage(queueName, settings))

}
