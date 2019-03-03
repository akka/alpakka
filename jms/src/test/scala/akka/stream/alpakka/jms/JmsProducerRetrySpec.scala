/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms

import java.util.concurrent.atomic.AtomicInteger

import akka.stream._
import akka.stream.alpakka.jms.scaladsl.{JmsConsumer, JmsProducer}
import akka.stream.scaladsl.{Keep, Sink, Source}
import javax.jms.{JMSException, Message, TextMessage}
import org.mockito.ArgumentMatchers.{any, anyInt, anyLong}
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

import scala.concurrent.duration._

class JmsProducerRetrySpec extends JmsSpec {

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(20.seconds)

  "JmsProducer retries" should {
    "retry sending on network failures" in withServer() { server =>
      val connectionFactory = server.createConnectionFactory
      val jms = JmsProducer.flow[JmsMapMessage](
        JmsProducerSettings(producerConfig, connectionFactory)
          .withQueue("test")
          .withSessionCount(3)
          .withConnectionRetrySettings(
            ConnectionRetrySettings(system)
              .withConnectTimeout(100.millis)
              .withInitialRetry(50.millis)
              .withMaxBackoff(50.millis)
              .withInfiniteRetries()
          )
          .withSendRetrySettings(
            SendRetrySettings(system).withInitialRetry(10.millis).withMaxBackoff(10.millis).withInfiniteRetries()
          )
      )

      val (queue, result) = Source
        .queue[Int](10, OverflowStrategy.backpressure)
        .zipWithIndex
        .map(e => JmsMapMessage(Map("time" -> System.currentTimeMillis(), "index" -> e._2)))
        .via(jms)
        .map(_.body)
        .toMat(Sink.seq)(Keep.both)
        .run()

      val sentResult = JmsConsumer
        .mapSource(JmsConsumerSettings(system, connectionFactory).withBufferSize(1).withQueue("test"))
        .take(20)
        .runWith(Sink.seq)

      for (_ <- 1 to 10) queue.offer(1) // 10 before the crash
      Thread.sleep(500)
      server.stop() // crash.

      Thread.sleep(1000)
      server.restart() // recover.
      val restartTime = System.currentTimeMillis()
      for (_ <- 1 to 10) queue.offer(1) // 10 after the crash
      queue.complete()

      val resultList = result.futureValue
      def index(m: Map[String, Any]) = m("index").asInstanceOf[Long]
      def time(m: Map[String, Any]) = m("time").asInstanceOf[Long]

      resultList.size shouldBe 20
      resultList.filter(b => time(b) >= restartTime) shouldNot be(empty)
      resultList.sliding(2).forall(pair => index(pair.head) + 1 == index(pair.last)) shouldBe true

      val sentList = sentResult.futureValue
      sentList.size shouldBe 20
      // all produced elements should have been sent to the consumer.
      resultList.forall { produced =>
        sentList.exists(consumed => index(consumed) == index(produced))
      } shouldBe true
    }

    "fail sending only after max retries" in withServer() { server =>
      val connectionFactory = server.createConnectionFactory
      val jms = JmsProducer.flow[JmsMapMessage](
        JmsProducerSettings(producerConfig, connectionFactory)
          .withQueue("test")
          .withConnectionRetrySettings(ConnectionRetrySettings(system).withInfiniteRetries())
          .withSendRetrySettings(
            SendRetrySettings(system)
              .withInitialRetry(100.millis)
              .withMaxBackoff(600.millis)
              .withBackoffFactor(2)
              .withMaxRetries(3)
          )
      )

      val (cancellable, result) = Source
        .tick(50.millis, 50.millis, "")
        .zipWithIndex
        .map(e => JmsMapMessage(Map("time" -> System.currentTimeMillis(), "index" -> e._2)))
        .via(jms)
        .map(_.body)
        .toMat(Sink.seq)(Keep.both)
        .run()

      Thread.sleep(500)
      val crashTime = System.currentTimeMillis()
      server.stop()
      val failure = result.failed.futureValue
      val failureTime = System.currentTimeMillis()

      val expectedDelay = 100L + 400L + 600L
      failureTime - crashTime shouldBe >(expectedDelay)
      failure shouldBe RetrySkippedOnMissingConnection
    }

    "fail immediately on non-recoverable errors" in withConnectionFactory() { connectionFactory =>
      val jms = JmsProducer.flow[JmsMapMessage](
        JmsProducerSettings(producerConfig, connectionFactory)
          .withQueue("test")
          .withSendRetrySettings(SendRetrySettings(system).withInfiniteRetries())
      )

      val result = Source(
        List(JmsMapMessage(Map("body" -> "1")), JmsMapMessage(Map("body" -> this)), JmsMapMessage(Map("body" -> "3")))
      ).via(jms)
        .map(_.body("body").toString)
        .runWith(Sink.seq)

      val failure = result.failed.futureValue
      failure shouldBe a[UnsupportedMapMessageEntryType]
    }

    "invoke supervisor when send fails" in withConnectionFactory() { connectionFactory =>
      val deciderCalls = new AtomicInteger()
      val decider: Supervision.Decider = { ex =>
        deciderCalls.incrementAndGet()
        Supervision.Resume
      }
      val settings = ActorMaterializerSettings(system).withSupervisionStrategy(decider)
      val materializer = ActorMaterializer(settings)(system)

      val jms = JmsProducer.flow[JmsMapMessage](
        JmsProducerSettings(producerConfig, connectionFactory)
          .withQueue("test")
          .withSendRetrySettings(SendRetrySettings(system).withInfiniteRetries())
      )

      // second element is a wrong map message.
      val result = Source(
        List(JmsMapMessage(Map("body" -> "1")), JmsMapMessage(Map("body" -> this)), JmsMapMessage(Map("body" -> "3")))
      ).via(jms)
        .map(_.body("body").toString)
        .runWith(Sink.seq)(materializer)

      // check that second element was skipped.
      val list = result.futureValue
      list shouldBe List("1", "3")

      deciderCalls.get shouldBe 1
    }

    "retry send as often as configured" in withMockedProducer { ctx =>
      import ctx._
      val sendAttempts = new AtomicInteger()
      val message = mock[TextMessage]

      when(session.createTextMessage(any[String])).thenReturn(message)

      when(producer.send(any[javax.jms.Destination], any[Message], anyInt(), anyInt(), anyLong()))
        .thenAnswer(new Answer[Unit]() {
          override def answer(invocation: InvocationOnMock): Unit = {
            sendAttempts.incrementAndGet()
            throw new JMSException("send error")
          }
        })

      val jms = JmsProducer.textSink(
        JmsProducerSettings(producerConfig, factory)
          .withQueue("test")
          .withSendRetrySettings(
            SendRetrySettings(system).withInitialRetry(10.millis).withMaxBackoff(10.millis).withMaxRetries(5)
          )
      )

      val result = Source(List("one")).runWith(jms)

      result.failed.futureValue shouldBe a[JMSException]
      sendAttempts.get shouldBe 6
    }

    "fail send on first attempt if retry is disabled" in withMockedProducer { ctx =>
      import ctx._
      val sendAttempts = new AtomicInteger()
      val message = mock[TextMessage]

      when(session.createTextMessage(any[String])).thenReturn(message)

      when(producer.send(any[javax.jms.Destination], any[Message], anyInt(), anyInt(), anyLong()))
        .thenAnswer(new Answer[Unit]() {
          override def answer(invocation: InvocationOnMock): Unit =
            if (sendAttempts.incrementAndGet() == 1) throw new JMSException("send error")
        })

      val jms = JmsProducer.textSink(
        JmsProducerSettings(producerConfig, factory)
          .withQueue("test")
          .withSendRetrySettings(SendRetrySettings(system).withMaxRetries(0))
      )

      val result = Source(List("one")).runWith(jms)

      result.failed.futureValue shouldBe a[JMSException]
      sendAttempts.get shouldBe 1
    }
  }
}
