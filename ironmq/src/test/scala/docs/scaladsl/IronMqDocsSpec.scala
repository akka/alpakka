/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import akka.stream.alpakka.ironmq.PushMessage
import akka.stream.alpakka.ironmq.impl.IronMqClientForTests
import akka.stream.alpakka.ironmq.scaladsl._
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.testkit.TestKit
import akka.{Done, NotUsed}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.BeforeAndAfterAll

import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class IronMqDocsSpec
    extends AnyWordSpec
    with IronMqClientForTests
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll
    with LogCapturing {

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(5.seconds, 250.millis)

  override def afterAll(): Unit =
    TestKit.shutdownActorSystem(system)

  "IronMqConsumer" should {
    "read messages" in assertAllStagesStopped {
      val queueName = givenQueue().futureValue
      import akka.stream.alpakka.ironmq.PushMessage

      val messages = (1 to 100).map(i => s"test-$i")
      val produced = Source(messages)
        .map(PushMessage(_))
        .runWith(IronMqProducer.sink(queueName, ironMqSettings))
      produced.futureValue shouldBe Done

      // #atMostOnce
      import akka.stream.alpakka.ironmq.Message

      val source: Source[Message, NotUsed] =
        IronMqConsumer.atMostOnceSource(queueName, ironMqSettings)

      val receivedMessages: Future[immutable.Seq[Message]] = source
        .take(100)
        .runWith(Sink.seq)
      // #atMostOnce
      receivedMessages.futureValue.map(_.body) should contain theSameElementsInOrderAs messages
    }

    "read messages and allow committing" in assertAllStagesStopped {
      val queueName = givenQueue().futureValue

      import akka.stream.alpakka.ironmq.PushMessage
      val messages = (1 to 100).map(i => s"test-$i")
      val produced = Source(messages)
        .map(PushMessage(_))
        .runWith(IronMqProducer.sink(queueName, ironMqSettings))
      produced.futureValue shouldBe Done

      // #atLeastOnce
      import akka.stream.alpakka.ironmq.scaladsl.CommittableMessage
      import akka.stream.alpakka.ironmq.Message

      val source: Source[CommittableMessage, NotUsed] =
        IronMqConsumer.atLeastOnceSource(queueName, ironMqSettings)

      val businessLogic: Flow[CommittableMessage, CommittableMessage, NotUsed] =
        Flow[CommittableMessage] // do something useful with the received messages

      val receivedMessages: Future[immutable.Seq[Message]] = source
        .take(100)
        .via(businessLogic)
        .mapAsync(1)(m => m.commit().map(_ => m.message))
        .runWith(Sink.seq)
      // #atLeastOnce
      receivedMessages.futureValue.map(_.body) should contain theSameElementsInOrderAs messages

    }
  }

  "IronMqProducer" should {
    "push messages in flow" in assertAllStagesStopped {
      val queueName = givenQueue().futureValue

      val messageCount = 10
      // #flow
      import akka.stream.alpakka.ironmq.{Message, PushMessage}

      val messages: immutable.Seq[String] = (1 to messageCount).map(i => s"test-$i")
      val producedIds: Future[immutable.Seq[Message.Id]] = Source(messages)
        .map(PushMessage(_))
        .via(IronMqProducer.flow(queueName, ironMqSettings))
        .runWith(Sink.seq)
      // #flow
      producedIds.futureValue.size shouldBe messageCount

      val receivedMessages: Future[immutable.Seq[Message]] = IronMqConsumer
        .atMostOnceSource(queueName, ironMqSettings)
        .take(messages.size)
        .runWith(Sink.seq)
      receivedMessages.futureValue.map(_.body) should contain theSameElementsInOrderAs messages

    }

    "push messages at-least-once" in assertAllStagesStopped {
      val sourceQueue = givenQueue().futureValue
      val targetQueue = givenQueue().futureValue

      val messageCount = 10
      val messages: immutable.Seq[String] = (1 to messageCount).map(i => s"test-$i")
      val produced = Source(messages)
        .map(PushMessage(_))
        .runWith(IronMqProducer.sink(sourceQueue, ironMqSettings))
      produced.futureValue shouldBe Done

      // #atLeastOnceFlow
      import akka.stream.alpakka.ironmq.{Message, PushMessage}
      import akka.stream.alpakka.ironmq.scaladsl.Committable

      val pushAndCommit: Flow[(PushMessage, Committable), Message.Id, NotUsed] =
        IronMqProducer.atLeastOnceFlow(targetQueue, ironMqSettings)

      val producedIds: Future[immutable.Seq[Message.Id]] = IronMqConsumer
        .atLeastOnceSource(sourceQueue, ironMqSettings)
        .take(messages.size)
        .map { committableMessage =>
          (PushMessage(committableMessage.message.body), committableMessage)
        }
        .via(pushAndCommit)
        .runWith(Sink.seq)
      // #atLeastOnceFlow
      producedIds.futureValue.size shouldBe messageCount

      val receivedMessages: Future[immutable.Seq[Message]] = IronMqConsumer
        .atMostOnceSource(targetQueue, ironMqSettings)
        .take(messages.size)
        .runWith(Sink.seq)
      receivedMessages.futureValue.map(_.body) should contain theSameElementsInOrderAs messages

    }

    "push messages to a sink" in assertAllStagesStopped {
      val queueName = givenQueue().futureValue

      val messageCount = 10
      // #sink
      import akka.stream.alpakka.ironmq.{Message, PushMessage}

      val messages: immutable.Seq[String] = (1 to messageCount).map(i => s"test-$i")
      val producedIds: Future[Done] = Source(messages)
        .map(PushMessage(_))
        .runWith(IronMqProducer.sink(queueName, ironMqSettings))
      // #sink
      producedIds.futureValue shouldBe Done

      val receivedMessages: Future[immutable.Seq[Message]] = IronMqConsumer
        .atMostOnceSource(queueName, ironMqSettings)
        .take(messages.size)
        .runWith(Sink.seq)
      receivedMessages.futureValue.map(_.body) should contain theSameElementsInOrderAs messages

    }
  }
}
