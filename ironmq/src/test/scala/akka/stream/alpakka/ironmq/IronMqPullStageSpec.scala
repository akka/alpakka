/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.ironmq

import akka.stream.scaladsl.{Sink, Source}
import scala.concurrent.ExecutionContext.Implicits.global

class IronMqPullStageSpec extends UnitSpec with IronMqFixture with AkkaStreamFixture {

  "IronMqSourceStage" when {
    "there are messages" should {
      "consume all messages" in {
        val queue = givenQueue()
        val messages = (1 to 100).map(i => PushMessage(s"test-$i"))
        ironMqClient.pushMessages(queue.name, messages: _*).futureValue

        val source = Source.fromGraph(new IronMqPullStage(queue.name, IronMqSettings()))
        val receivedMessages = source.take(100).runWith(Sink.seq).map(_.map(_.message.body)).futureValue
        val expectedMessages = messages.map(_.body)

        receivedMessages should contain theSameElementsInOrderAs expectedMessages
      }
    }
  }

}
