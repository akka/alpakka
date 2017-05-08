/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.ironmq

import akka.dispatch.ExecutionContexts
import akka.stream.scaladsl._

import scala.concurrent.ExecutionContext

class IronMqPushStageSpec extends UnitSpec with IronMqFixture with AkkaStreamFixture {

  implicit val ec: ExecutionContext = ExecutionContexts.global()

  "IronMqPushMessageStage" should {
    "push messages to the queue" in {

      val queue = givenQueue()
      val flow = Flow.fromGraph(new IronMqPushStage(queue.name, IronMqSettings()))

      val expectedMessagesBodies = List("test-1", "test-2")

      val producedMessagesIds = Source(expectedMessagesBodies)
        .map(PushMessage(_))
        .via(flow)
        .mapAsync(2)(identity)
        .mapConcat(_.ids)
        .toMat(Sink.seq)(Keep.right)
        .run()
        .futureValue

      val consumedMessagesIds = ironMqClient.pullMessages(queue.name, 20).futureValue.map(_.messageId).toSeq

      consumedMessagesIds should contain theSameElementsAs producedMessagesIds
    }
  }

}
