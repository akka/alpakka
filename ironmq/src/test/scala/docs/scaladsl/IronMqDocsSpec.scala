/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl
import akka.stream.{FlowShape, Graph}
import akka.stream.alpakka.ironmq.PushMessage
import akka.stream.alpakka.ironmq.impl.IronMqClientForTests
import akka.stream.alpakka.ironmq.scaladsl._
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.testkit.TestKit
import akka.{Done, NotUsed}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration._

class IronMqDocsSpec extends WordSpec with IronMqClientForTests with Matchers with ScalaFutures with BeforeAndAfterAll {

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(5.seconds, 250.millis)

  override def afterAll() =
    TestKit.shutdownActorSystem(system)

  "IronMqConsumer" should {
    "read messages" in {
      // #atMostOnce
      import akka.stream.alpakka.ironmq.{Message, Queue}

      val queue: Queue = Queue.name("alpakka-sample")
      // #atMostOnce
      givenQueue(queue.name).futureValue
      import akka.stream.alpakka.ironmq.PushMessage

      val messages = (1 to 100).map(i => s"test-$i")
      val produced = Source(messages)
        .map(PushMessage(_))
        .runWith(IronMqProducer.producerSink(queue.name, ironMqSettings))
      produced.futureValue shouldBe Done

      // #atMostOnce
      val source: Source[Message, NotUsed] =
        IronMqConsumer.atMostOnceConsumerSource(queue.name, ironMqSettings)

      val receivedMessages: Future[immutable.Seq[Message]] = source
        .take(100)
        .runWith(Sink.seq)
      // #atMostOnce
      receivedMessages.futureValue.map(_.body) should contain theSameElementsInOrderAs messages
    }

    "read messages and allow committing" in {
      // #atLeastOnce
      import akka.stream.alpakka.ironmq.{Message, Queue}
      import akka.stream.alpakka.ironmq.scaladsl.CommittableMessage

      val queue: Queue = Queue.name("alpakka-committing")
      // #atLeastOnce
      givenQueue(queue.name).futureValue

      import akka.stream.alpakka.ironmq.PushMessage
      val messages = (1 to 100).map(i => s"test-$i")
      val produced = Source(messages)
        .map(PushMessage(_))
        .runWith(IronMqProducer.producerSink(queue.name, ironMqSettings))
      produced.futureValue shouldBe Done

      // #atLeastOnce

      val source: Source[CommittableMessage, NotUsed] =
        IronMqConsumer.atLeastOnceConsumerSource(queue.name, ironMqSettings)

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
    "push messages in flow" in {
      import akka.stream.alpakka.ironmq.Queue
      val queue: Queue = givenQueue().futureValue

      val messageCount = 10
      // #flow
      import akka.stream.alpakka.ironmq.Message
      import akka.stream.alpakka.ironmq.PushMessage

      val messages: immutable.Seq[String] = (1 to messageCount).map(i => s"test-$i")
      val producedIds: Future[immutable.Seq[Message.Id]] = Source(messages)
        .map(PushMessage(_))
        .via(IronMqProducer.producerFlow(queue.name, ironMqSettings))
        .runWith(Sink.seq)
      // #flow
      producedIds.futureValue.size shouldBe messageCount

      val receivedMessages: Future[immutable.Seq[Message]] = IronMqConsumer
        .atMostOnceConsumerSource(queue.name, ironMqSettings)
        .take(messages.size)
        .runWith(Sink.seq)
      receivedMessages.futureValue.map(_.body) should contain theSameElementsInOrderAs messages

    }

    "push messages at-least-once" in {
      import akka.stream.alpakka.ironmq.Queue
      val sourceQueue: Queue = givenQueue().futureValue
      val targetQueue: Queue = givenQueue().futureValue

      val messageCount = 10
      val messages: immutable.Seq[String] = (1 to messageCount).map(i => s"test-$i")
      val produced = Source(messages)
        .map(PushMessage(_))
        .runWith(IronMqProducer.producerSink(sourceQueue.name, ironMqSettings))
      produced.futureValue shouldBe Done

      // #atLeastOnceFlow
      import akka.stream.alpakka.ironmq.Message
      import akka.stream.alpakka.ironmq.PushMessage
      import akka.stream.alpakka.ironmq.scaladsl.Committable

      val pushAndCommit: Flow[(PushMessage, Committable), Message.Id, NotUsed] =
        IronMqProducer.atLeastOnceProducerFlow(targetQueue.name, ironMqSettings)

      val producedIds: Future[immutable.Seq[Message.Id]] = IronMqConsumer
        .atLeastOnceConsumerSource(sourceQueue.name, ironMqSettings)
        .take(messages.size)
        .map { committableMessage =>
          (PushMessage(committableMessage.message.body), committableMessage)
        }
        .via(pushAndCommit)
        .runWith(Sink.seq)
      // #atLeastOnceFlow
      producedIds.futureValue.size shouldBe messageCount

      val receivedMessages: Future[immutable.Seq[Message]] = IronMqConsumer
        .atMostOnceConsumerSource(targetQueue.name, ironMqSettings)
        .take(messages.size)
        .runWith(Sink.seq)
      receivedMessages.futureValue.map(_.body) should contain theSameElementsInOrderAs messages

    }

    "push messages to a sink" in {
      import akka.stream.alpakka.ironmq.Queue
      val queue: Queue = givenQueue().futureValue

      val messageCount = 10
      // #sink
      import akka.stream.alpakka.ironmq.Message
      import akka.stream.alpakka.ironmq.PushMessage

      val messages: immutable.Seq[String] = (1 to messageCount).map(i => s"test-$i")
      val producedIds: Future[Done] = Source(messages)
        .map(PushMessage(_))
        .runWith(IronMqProducer.producerSink(queue.name, ironMqSettings))
      // #sink
      producedIds.futureValue shouldBe Done

      val receivedMessages: Future[immutable.Seq[Message]] = IronMqConsumer
        .atMostOnceConsumerSource(queue.name, ironMqSettings)
        .take(messages.size)
        .runWith(Sink.seq)
      receivedMessages.futureValue.map(_.body) should contain theSameElementsInOrderAs messages

    }
  }
}
