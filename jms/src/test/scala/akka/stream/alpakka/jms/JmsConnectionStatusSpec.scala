/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.jms
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger

import akka.stream.OverflowStrategy
import akka.stream.alpakka.jms.scaladsl.JmsConnectorState._
import akka.stream.alpakka.jms.scaladsl.{JmsConnectorState, JmsConsumer, JmsProducer, JmsProducerStatus}
import akka.stream.scaladsl.{Flow, Keep, Sink, SinkQueueWithCancel, Source}
import javax.jms._
import org.mockito.ArgumentMatchers.{any, anyBoolean, anyInt}
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.matchers.{MatchResult, Matcher}

import scala.concurrent.duration._

class JmsConnectionStatusSpec extends JmsSpec {

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(10.seconds, 50.millis)

  "JmsConnector connection status" should {

    "report disconnected on producer stream failure" in withConnectionFactory() { connectionFactory =>
      val connectedLatch = new CountDownLatch(1)

      val jmsSink = textSink(JmsProducerSettings(producerConfig, connectionFactory).withQueue("test"))
      val exception = new RuntimeException("failing stage")

      val producerStatus = Source
        .tick(10.millis, 20.millis, "text")
        .zipWithIndex
        .map { x =>
          if (x._2 == 3) {
            connectedLatch.await()
            throw exception
          }
          x._1
        }
        .runWith(jmsSink)

      val status = producerStatus.connectorState.toMat(Sink.queue())(Keep.right).run()

      status should havePublishedState(Connecting(1))
      status should havePublishedState(Connected)

      connectedLatch.countDown()

      status should havePublishedState(Failing(exception))
      status should havePublishedState(Failed(exception))
    }

    "report multiple connection attempts" in withMockedProducer { ctx =>
      import ctx._
      val connectAttempts = new AtomicInteger()
      when(factory.createConnection()).thenAnswer(new Answer[Connection]() {
        override def answer(invocation: InvocationOnMock): Connection =
          if (connectAttempts.getAndIncrement() == 0) throw new JMSException("connect error") else connection
      })

      val jmsSink = textSink(JmsProducerSettings(producerConfig, factory).withQueue("test"))
      val connectionStatus = Source.tick(10.millis, 20.millis, "text").runWith(jmsSink).connectorState
      val status = connectionStatus.runWith(Sink.queue())

      status should havePublishedState(Connecting(1))
      status should havePublishedState(Disconnected)
      status should havePublishedState(Connecting(2))
      status should havePublishedState(Connected)
    }

    "report failure when running out of connection attempts" in withMockedProducer { ctx =>
      import ctx._
      val connectAttempts = new AtomicInteger()
      val exception = new JMSException("connect error")
      val retryException = ConnectionRetryException("Could not establish connection after 1 retries.", exception)
      when(factory.createConnection()).thenAnswer(new Answer[Connection]() {
        override def answer(invocation: InvocationOnMock): Connection =
          if (connectAttempts.getAndIncrement() < 2) throw exception else connection
      })

      val jmsSink = textSink(
        JmsProducerSettings(producerConfig, factory)
          .withConnectionRetrySettings(ConnectionRetrySettings(system).withMaxRetries(1))
          .withQueue("test")
      )
      val connectionStatus = Source.tick(10.millis, 20.millis, "text").runWith(jmsSink).connectorState
      val status = connectionStatus.runWith(Sink.queue())

      status should havePublishedState(Connecting(1))
      status should havePublishedState(Disconnected)
      status should havePublishedState(Connecting(2))
      status should havePublishedState(Failing(retryException))
      status should havePublishedState(Failed(retryException))
    }

    "retry connection when creating session fails" in withMockedProducer { ctx =>
      import ctx._
      val connectAttempts = new AtomicInteger()
      when(connection.createSession(anyBoolean(), anyInt())).thenAnswer(new Answer[Session]() {
        override def answer(invocation: InvocationOnMock): Session =
          if (connectAttempts.getAndIncrement() == 0) throw new JMSException("connect error") else session
      })

      val jmsSink = textSink(JmsProducerSettings(producerConfig, factory).withQueue("test"))
      val connectionStatus = Source.tick(10.millis, 20.millis, "text").runWith(jmsSink).connectorState
      val status = connectionStatus.runWith(Sink.queue())

      status should havePublishedState(Connecting(1))
      status should havePublishedState(Disconnected)
      status should havePublishedState(Connecting(2))
      status should havePublishedState(Connected)
    }

    "abort connection on security exception" in withMockedProducer { ctx =>
      import ctx._
      val connectAttempts = new AtomicInteger()
      val securityException = new JMSSecurityException("security error")
      when(factory.createConnection()).thenAnswer(new Answer[Connection]() {
        override def answer(invocation: InvocationOnMock): Connection =
          if (connectAttempts.getAndIncrement() == 0) throw securityException else connection
      })

      val jmsSink = textSink(JmsProducerSettings(producerConfig, factory).withQueue("test"))
      val connectionStatus = Source.tick(10.millis, 20.millis, "text").runWith(jmsSink).connectorState
      val status = connectionStatus.runWith(Sink.queue())

      eventually { status should havePublishedState(Failing(securityException)) }
      status should havePublishedState(Failed(securityException))
    }

    "retry connection when creating producer fails" in withMockedProducer { ctx =>
      import ctx._
      val connectAttempts = new AtomicInteger()
      when(session.createProducer(any[javax.jms.Destination])).thenAnswer(new Answer[MessageProducer]() {
        override def answer(invocation: InvocationOnMock): MessageProducer =
          if (connectAttempts.getAndIncrement() == 0) throw new JMSException("connect error") else producer
      })

      val jmsSink = textSink(JmsProducerSettings(producerConfig, factory).withQueue("test"))
      val connectionStatus = Source.tick(10.millis, 20.millis, "text").runWith(jmsSink).connectorState
      val status = connectionStatus.runWith(Sink.queue())

      status should havePublishedState(Connecting(1))
      status should havePublishedState(Disconnected)
      status should havePublishedState(Connecting(2))
      status should havePublishedState(Connected)
    }

    "retry connection when setting exception listener fails" in withMockedProducer { ctx =>
      import ctx._
      val connectAttempts = new AtomicInteger()
      when(connection.setExceptionListener(any[ExceptionListener]())).thenAnswer(new Answer[Unit]() {
        override def answer(invocation: InvocationOnMock): Unit =
          if (connectAttempts.getAndIncrement() == 0) throw new JMSException("connect error") else ()
      })

      val jmsSink = textSink(JmsProducerSettings(producerConfig, factory).withQueue("test"))
      val connectionStatus = Source.tick(10.millis, 20.millis, "text").runWith(jmsSink).connectorState
      val status = connectionStatus.runWith(Sink.queue())

      status should havePublishedState(Connecting(1))
      status should havePublishedState(Disconnected)
      status should havePublishedState(Connecting(2))
      status should havePublishedState(Connected)
    }

    "retry connection when creating producer destination fails" in withMockedProducer { ctx =>
      import ctx._
      val connectAttempts = new AtomicInteger()
      when(session.createQueue(any[String])).thenAnswer(new Answer[javax.jms.Queue]() {
        override def answer(invocation: InvocationOnMock): javax.jms.Queue =
          if (connectAttempts.getAndIncrement() == 0) throw new JMSException("connect error") else queue
      })

      val jmsSink = textSink(JmsProducerSettings(producerConfig, factory).withQueue("test"))
      val connectionStatus = Source.tick(10.millis, 20.millis, "text").runWith(jmsSink).connectorState
      val status = connectionStatus.runWith(Sink.queue())

      status should havePublishedState(Connecting(1))
      status should havePublishedState(Disconnected)
      status should havePublishedState(Connecting(2))
      status should havePublishedState(Connected)
    }

    "retry connection when creating consumer fails" in withMockedConsumer { ctx =>
      import ctx._
      val connectAttempts = new AtomicInteger()
      when(session.createConsumer(any[javax.jms.Destination])).thenAnswer(new Answer[MessageConsumer]() {
        override def answer(invocation: InvocationOnMock): MessageConsumer =
          if (connectAttempts.getAndIncrement() == 0) throw new JMSException("connect error") else consumer
      })

      val jmsSource = JmsConsumer.textSource(JmsConsumerSettings(system, factory).withQueue("test"))
      val connectionStatus = jmsSource.toMat(Sink.ignore)(Keep.left).run().connectorState
      val status = connectionStatus.runWith(Sink.queue())

      status should havePublishedState(Connecting(1))
      status should havePublishedState(Disconnected)
      status should havePublishedState(Connecting(2))
      status should havePublishedState(Connected)
    }

    "retry connection when creating consumer destination fails" in withMockedConsumer { ctx =>
      import ctx._
      val connectAttempts = new AtomicInteger()
      when(session.createQueue(any[String])).thenAnswer(new Answer[javax.jms.Queue]() {
        override def answer(invocation: InvocationOnMock): javax.jms.Queue =
          if (connectAttempts.getAndIncrement() == 0) throw new JMSException("connect error") else queue
      })

      val jmsSource = JmsConsumer.textSource(JmsConsumerSettings(system, factory).withQueue("test"))
      val connectionStatus = jmsSource.toMat(Sink.ignore)(Keep.left).run().connectorState
      val status = connectionStatus.runWith(Sink.queue())

      status should havePublishedState(Connecting(1))
      status should havePublishedState(Disconnected)
      status should havePublishedState(Connecting(2))
      status should havePublishedState(Connected)
    }

    "report disconnected on consumer stream failure" in withConnectionFactory() { connectionFactory =>
      val connectedLatch = new CountDownLatch(1)
      val exception = new RuntimeException("failing stage")

      val jmsSink = textSink(JmsProducerSettings(producerConfig, connectionFactory).withQueue("test"))

      val jmsSource = JmsConsumer.textSource(JmsConsumerSettings(system, connectionFactory).withQueue("test"))

      val consumerControl = jmsSource.zipWithIndex
        .map { x =>
          if (x._2 == 3) {
            connectedLatch.await()
            throw exception
          }
          x._1
        }
        .toMat(Sink.ignore)(Keep.left)
        .run()

      Source.tick(10.millis, 20.millis, "text").runWith(jmsSink)

      val status = consumerControl.connectorState.runWith(Sink.queue())

      eventually { status should havePublishedState(Connected) }

      connectedLatch.countDown()

      eventually { status should havePublishedState(Completing) }
      eventually { status should havePublishedState(Completed) }
    }

    "report completion on stream shutdown / cancel" in withConnectionFactory() { connectionFactory =>
      val jmsSink = textSink(
        JmsProducerSettings(producerConfig, connectionFactory)
          .withQueue("test")
      )

      val jmsSource = JmsConsumer.textSource(
        JmsConsumerSettings(system, connectionFactory)
          .withQueue("test")
      )

      val (cancellable, producerStatus) = Source.tick(10.millis, 20.millis, "text").toMat(jmsSink)(Keep.both).run()
      val consumerControl = jmsSource.toMat(Sink.ignore)(Keep.left).run()

      val consumerConnected = consumerControl.connectorState.runWith(Sink.queue())
      val producerConnected = producerStatus.connectorState.runWith(Sink.queue())

      eventually { consumerConnected should havePublishedState(Connected) }
      eventually { producerConnected should havePublishedState(Connected) }

      cancellable.cancel()
      consumerControl.shutdown()

      eventually { consumerConnected should havePublishedState(Completing) }
      eventually { consumerConnected should havePublishedState(Completed) }

      eventually { producerConnected should havePublishedState(Completing) }
      eventually { producerConnected should havePublishedState(Completed) }
    }

    "report failure on external stream abort" in withConnectionFactory() { connectionFactory =>
      val exception = new RuntimeException("aborting stream")

      val jmsSource = JmsConsumer.textSource(
        JmsConsumerSettings(system, connectionFactory)
          .withQueue("test")
      )

      val consumerControl = jmsSource.toMat(Sink.ignore)(Keep.left).run()

      val consumerConnected = consumerControl.connectorState.runWith(Sink.queue())

      eventually { consumerConnected should havePublishedState(Connected) }

      consumerControl.abort(exception)

      consumerConnected should havePublishedState(Failing(exception))
      consumerConnected should havePublishedState(Failed(exception))
    }

    "reflect connection status on connection retries" in withServer() { server =>
      val connectionFactory = server.createConnectionFactory
      val jmsSink = textSink(
        JmsProducerSettings(producerConfig, connectionFactory)
          .withQueue("test")
          .withConnectionRetrySettings(
            ConnectionRetrySettings(system)
              .withConnectTimeout(1.second)
              .withInitialRetry(100.millis)
              .withMaxBackoff(100.millis)
              .withInfiniteRetries()
          )
          .withSendRetrySettings(SendRetrySettings(system).withInfiniteRetries())
      )

      val jmsSource = JmsConsumer.textSource(
        JmsConsumerSettings(system, connectionFactory)
          .withQueue("test")
          .withConnectionRetrySettings(
            ConnectionRetrySettings(system)
              .withConnectTimeout(1.second)
              .withInitialRetry(100.millis)
              .withMaxBackoff(100.millis)
              .withInfiniteRetries()
          )
      )

      val producerStatus = Source.tick(50.millis, 100.millis, "text").runWith(jmsSink)
      val consumerControl = jmsSource.toMat(Sink.ignore)(Keep.left).run()

      val consumerConnected =
        consumerControl.connectorState.buffer(10, OverflowStrategy.backpressure).runWith(Sink.queue())
      val producerConnected =
        producerStatus.connectorState.buffer(10, OverflowStrategy.backpressure).runWith(Sink.queue())

      for (_ <- 1 to 20) {
        eventually { consumerConnected should havePublishedState(Connected) }
        eventually { producerConnected should havePublishedState(Connected) }

        server.stop()

        eventually { consumerConnected should havePublishedState(Disconnected) }
        eventually { producerConnected should havePublishedState(Disconnected) }

        server.restart()
      }

      eventually { consumerConnected should havePublishedState(Connected) }
      eventually { producerConnected should havePublishedState(Connected) }
    }
  }

  private def textSink(settings: JmsProducerSettings): Sink[String, JmsProducerStatus] =
    Flow[String]
      .map(s => JmsTextMessage(s))
      .viaMat(JmsProducer.flow(settings))(Keep.right)
      .to(Sink.ignore)

  class ConnectionStateMatcher(expectedState: JmsConnectorState)
      extends Matcher[SinkQueueWithCancel[JmsConnectorState]] {

    def apply(queue: SinkQueueWithCancel[JmsConnectorState]): MatchResult =
      queue.pull().futureValue match {
        case Some(state) =>
          MatchResult(
            state == expectedState,
            s"""Published connection state $state was not $expectedState""",
            s"""Published connection state $state was $expectedState"""
          )
        case None =>
          MatchResult(
            matches = false,
            s"""Did not publish connection state. Expected was $expectedState""",
            s"""Published connection state"""
          )
      }
  }

  def havePublishedState(expectedState: JmsConnectorState) =
    new ConnectionStateMatcher(expectedState: JmsConnectorState)
}
