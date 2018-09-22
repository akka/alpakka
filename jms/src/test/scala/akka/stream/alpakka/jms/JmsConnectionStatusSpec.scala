/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms
import java.util.concurrent.CountDownLatch

import akka.NotUsed
import akka.stream.OverflowStrategy
import akka.stream.alpakka.jms.scaladsl.JmsConnectorState._
import akka.stream.alpakka.jms.scaladsl.{JmsConsumer, JmsProducer, JmsProducerStatus}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import org.apache.activemq.ActiveMQConnectionFactory

import scala.concurrent.duration._

class JmsConnectionStatusSpec extends JmsSpec {

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(10.seconds, 50.millis)

  private def textSink(settings: JmsProducerSettings): Sink[String, JmsProducerStatus] =
    Flow[String]
      .map(s => JmsProducerMessage.message(JmsTextMessage(s), NotUsed))
      .viaMat(JmsProducer.flexiFlow(settings))(Keep.right)
      .to(Sink.ignore)

  "JmsConnector connection status" should {

    "report disconnected on producer stream failure" in withServer() { ctx =>
      val connectionFactory: javax.jms.ConnectionFactory = new ActiveMQConnectionFactory(ctx.url)
      val connectedLatch = new CountDownLatch(1)

      val jmsSink = textSink(JmsProducerSettings(connectionFactory).withQueue("test"))

      val producerStatus = Source
        .tick(10.millis, 20.millis, "text")
        .zipWithIndex
        .map { x =>
          if (x._2 == 3) {
            connectedLatch.await()
            throw new RuntimeException("failing stage")
          }
          x._1
        }
        .runWith(jmsSink)

      val status = producerStatus.connection.toMat(Sink.queue())(Keep.right).run()

      eventually { status.pull().futureValue shouldBe Some(Connected) }

      connectedLatch.countDown()

      eventually { status.pull().futureValue shouldBe Some(Stopping) }
    }

    "report disconnected on consumer stream failure" in withServer() { ctx =>
      val connectionFactory: javax.jms.ConnectionFactory = new ActiveMQConnectionFactory(ctx.url)
      val connectedLatch = new CountDownLatch(1)

      val jmsSink = textSink(JmsProducerSettings(connectionFactory).withQueue("test"))

      val jmsSource = JmsConsumer.textSource(JmsConsumerSettings(connectionFactory).withQueue("test"))

      val consumerControl = jmsSource.zipWithIndex
        .map { x =>
          if (x._2 == 3) {
            connectedLatch.await()
            throw new RuntimeException("failing stage")
          }
          x._1
        }
        .toMat(Sink.ignore)(Keep.left)
        .run()

      Source.tick(10.millis, 20.millis, "text").runWith(jmsSink)

      val status = consumerControl.connection.toMat(Sink.queue())(Keep.right).run()

      eventually { status.pull().futureValue shouldBe Some(Connected) }

      connectedLatch.countDown()

      eventually { status.pull().futureValue shouldBe Some(Stopping) }
    }

    "report disconnected on stream completion" in withServer() { ctx =>
      val connectionFactory: javax.jms.ConnectionFactory = new ActiveMQConnectionFactory(ctx.url)

      val jmsSink = textSink(
        JmsProducerSettings(connectionFactory)
          .withQueue("test")
      )

      val jmsSource = JmsConsumer.textSource(
        JmsConsumerSettings(connectionFactory)
          .withQueue("test")
      )

      val (cancellable, producerStatus) = Source.tick(10.millis, 20.millis, "text").toMat(jmsSink)(Keep.both).run()
      val consumerControl = jmsSource.toMat(Sink.ignore)(Keep.left).run()

      val consumerConnected = consumerControl.connection.toMat(Sink.queue())(Keep.right).run()
      val producerConnected = producerStatus.connection.toMat(Sink.queue())(Keep.right).run()

      eventually { consumerConnected.pull().futureValue shouldBe Some(Connected) }
      eventually { producerConnected.pull().futureValue shouldBe Some(Connected) }

      cancellable.cancel()
      consumerControl.shutdown()

      eventually { consumerConnected.pull().futureValue shouldBe Some(Stopping) }
      eventually { producerConnected.pull().futureValue shouldBe Some(Stopping) }
    }

    "reflect connection status on connection retries" in withServer() { ctx =>
      val connectionFactory: javax.jms.ConnectionFactory = new ActiveMQConnectionFactory(ctx.url)

      val jmsSink = textSink(
        JmsProducerSettings(connectionFactory)
          .withQueue("test")
          .withConnectionRetrySettings(
            ConnectionRetrySettings()
              .withConnectTimeout(1.second)
              .withInitialRetry(100.millis)
              .withMaxBackoff(100.millis)
              .withInfiniteRetries()
          )
      )

      val jmsSource = JmsConsumer.textSource(
        JmsConsumerSettings(connectionFactory)
          .withQueue("test")
          .withConnectionRetrySettings(
            ConnectionRetrySettings()
              .withConnectTimeout(1.second)
              .withInitialRetry(100.millis)
              .withMaxBackoff(100.millis)
              .withInfiniteRetries()
          )
      )

      val producerStatus = Source
        .tick(50.millis, 100.millis, "text")
        .toMat(jmsSink)(Keep.right)
        .run()
      val consumerControl = jmsSource.toMat(Sink.ignore)(Keep.left).run()

      val consumerConnected =
        consumerControl.connection.buffer(10, OverflowStrategy.backpressure).toMat(Sink.queue())(Keep.right).run()
      val producerConnected =
        producerStatus.connection.buffer(10, OverflowStrategy.backpressure).toMat(Sink.queue())(Keep.right).run()

      for (_ <- 1 to 20) {
        eventually { consumerConnected.pull().futureValue shouldBe Some(Connected) }
        eventually { producerConnected.pull().futureValue shouldBe Some(Connected) }

        ctx.broker.stop()

        eventually { consumerConnected.pull().futureValue shouldBe Some(Disconnected) }
        eventually { producerConnected.pull().futureValue shouldBe Some(Disconnected) }

        ctx.broker.start(true)
      }

      eventually { consumerConnected.pull().futureValue shouldBe Some(Connected) }
      eventually { producerConnected.pull().futureValue shouldBe Some(Connected) }
    }
  }
}
