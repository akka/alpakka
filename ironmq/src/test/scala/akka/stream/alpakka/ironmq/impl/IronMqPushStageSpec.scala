/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.ironmq.impl

import akka.dispatch.ExecutionContexts
import akka.stream.alpakka.ironmq.{IronMqSettings, IronMqSpec, PushMessage}
import akka.stream.scaladsl._
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped

import scala.concurrent.ExecutionContext

class IronMqPushStageSpec extends IronMqSpec {

  implicit val ec: ExecutionContext = ExecutionContexts.global()

  "IronMqPushMessageStage" should {
    "push messages to the queue" in assertAllStagesStopped {

      val queueName = givenQueue()
      val flow = Flow.fromGraph(new IronMqPushStage(queueName, IronMqSettings()))

      val expectedMessagesBodies = List("test-1", "test-2")

      val producedMessagesIds = Source(expectedMessagesBodies)
        .map(PushMessage(_))
        .via(flow)
        .mapAsync(2)(identity)
        .mapConcat(_.ids)
        .toMat(Sink.seq)(Keep.right)
        .run()
        .futureValue

      val consumedMessagesIds = ironMqClient.pullMessages(queueName, 20).futureValue.map(_.messageId).toSeq

      consumedMessagesIds should contain theSameElementsAs producedMessagesIds
    }
  }

}
