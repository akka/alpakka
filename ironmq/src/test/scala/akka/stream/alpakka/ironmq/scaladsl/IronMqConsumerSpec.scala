/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.ironmq.scaladsl

import akka.NotUsed
import akka.dispatch.ExecutionContexts
import akka.stream.alpakka.ironmq.{AkkaStreamFixture, IronMqFixture, IronMqSettings, PushMessage, UnitSpec}
import akka.stream.scaladsl.{Sink, Source}
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.ParallelTestExecution

import scala.concurrent.ExecutionContext

class IronMqConsumerSpec extends UnitSpec with IronMqFixture with AkkaStreamFixture with ParallelTestExecution {

  implicit val ec: ExecutionContext = ExecutionContexts.global()

  val messages: Source[PushMessage, NotUsed] =
    Source.fromIterator(() => Iterator.from(0)).map(i => PushMessage(s"test-$i"))

  override protected def initConfig(): Config =
    ConfigFactory.parseString(s"""akka.stream.alpakka.ironmq {
         |  consumer.reservation-timeout = 30 seconds
         |}
      """.stripMargin).withFallback(super.initConfig())

  "atLeastOnceConsumerSource" should {
    "not delete messages from the queue if not committed" in {
      val queue = givenQueue()
      val numberOfMessages = 10

      messages
        .take(numberOfMessages)
        .mapAsync(1)(ironMqClient.pushMessages(queue.name, _))
        .runWith(Sink.ignore)
        .futureValue

      IronMqConsumer
        .atLeastOnceConsumerSource(queue.name, IronMqSettings())
        .take(numberOfMessages)
        .runWith(Sink.ignore)
        .futureValue

      ironMqClient.peekMessages(queue.name, 100).futureValue shouldBe empty

      // Sleep enough time to be sure the messages has been put back in queue by IronMQ
      Thread.sleep(45000L)

      ironMqClient.peekMessages(queue.name, 100).futureValue should have size numberOfMessages
    }

    "delete the messages from the queue when committed" in {
      val queue = givenQueue()
      val numberOfMessages = 10

      messages
        .take(numberOfMessages)
        .mapAsync(1)(ironMqClient.pushMessages(queue.name, _))
        .runWith(Sink.ignore)
        .futureValue

      IronMqConsumer
        .atLeastOnceConsumerSource(queue.name, IronMqSettings())
        .take(numberOfMessages)
        .mapAsync(3)(_.commit())
        .runWith(Sink.ignore)
        .futureValue

      ironMqClient.peekMessages(queue.name, 100).futureValue shouldBe empty

      // Sleep enough time to be sure the messages may have been put back in queue by IronMQ
      Thread.sleep(45000L)

      ironMqClient.peekMessages(queue.name, 100).futureValue shouldBe empty
    }

  }

}
