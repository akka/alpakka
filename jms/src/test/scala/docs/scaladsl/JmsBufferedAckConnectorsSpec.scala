/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

import akka.Done
import akka.stream.alpakka.jms._
import akka.stream.alpakka.jms.scaladsl.{JmsConsumer, JmsConsumerControl, JmsProducer}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.stream.testkit.scaladsl.TestSink
import akka.stream.{KillSwitches, ThrottleMode}
import org.apache.activemq.ActiveMQSession
import javax.jms.{JMSException, TextMessage}
import org.scalatest.Inspectors._
import org.scalatest.time.Span.convertSpanToDuration

import scala.annotation.tailrec
import scala.collection.{immutable, mutable}
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

class JmsBufferedAckConnectorsSpec extends JmsSharedServerSpec {

  override implicit val patienceConfig = PatienceConfig(2.minutes)

  "The JMS Ack Connectors" should {
    "publish and consume strings through a queue" in withConnectionFactory() { connectionFactory =>
      val queueName = createName("test")
      val jmsSink: Sink[String, Future[Done]] = JmsProducer.textSink(
        JmsProducerSettings(producerConfig, connectionFactory).withQueue(queueName)
      )

      val in = 0 to 25 map (i => ('a' + i).asInstanceOf[Char].toString)
      Source(in).runWith(jmsSink)

      val jmsSource: Source[AckEnvelope, JmsConsumerControl] = JmsConsumer.ackSource(
        JmsConsumerSettings(system, connectionFactory).withSessionCount(5).withQueue(queueName)
      )

      val result = jmsSource
        .take(in.size)
        .map(env => (env, env.message.asInstanceOf[TextMessage].getText))
        .map { case (env, text) => env.acknowledge(); text }
        .runWith(Sink.seq)

      result.futureValue should contain theSameElementsAs in
    }

    "publish and consume JMS text messages with properties through a queue" in withConnectionFactory() {
      connectionFactory =>
        val queueName = createName("numbers")
        val jmsSink: Sink[JmsTextMessage, Future[Done]] = JmsProducer.sink(
          JmsProducerSettings(producerConfig, connectionFactory).withQueue(queueName)
        )

        val msgsIn = 1 to 100 map { n =>
          JmsTextMessage(n.toString)
            .withProperty("Number", n)
            .withProperty("IsOdd", n % 2 == 1)
            .withProperty("IsEven", n % 2 == 0)
        }

        Source(msgsIn).runWith(jmsSink)

        //#source
        val jmsSource: Source[AckEnvelope, JmsConsumerControl] = JmsConsumer.ackSource(
          JmsConsumerSettings(consumerConfig, connectionFactory)
            .withSessionCount(5)
            .withQueue(queueName)
        )

        val result: Future[immutable.Seq[javax.jms.Message]] =
          jmsSource
            .take(msgsIn.size)
            .map { ackEnvelope =>
              ackEnvelope.acknowledge()
              ackEnvelope.message
            }
            .runWith(Sink.seq)
        //#source

        // The sent message and the receiving one should have the same properties
        val sortedResult = result.futureValue.sortBy(msg => msg.getIntProperty("Number"))
        forAll(sortedResult.zip(msgsIn)) {
          case (out, in) =>
            out.getIntProperty("Number") shouldEqual in.properties("Number")
            out.getBooleanProperty("IsOdd") shouldEqual in.properties("IsOdd")
            out.getBooleanProperty("IsEven") shouldEqual in.properties("IsEven")
        }
    }

    "publish JMS text messages with properties through a queue and consume them with a selector" in withServer() {
      server =>
        val connectionFactory = server.createConnectionFactory
        val jmsSink: Sink[JmsTextMessage, Future[Done]] = JmsProducer.sink(
          JmsProducerSettings(producerConfig, connectionFactory).withQueue("numbers")
        )

        val msgsIn = 1 to 100 map { n =>
          JmsTextMessage(n.toString)
            .withProperty("Number", n)
            .withProperty("IsOdd", n % 2 == 1)
            .withProperty("IsEven", n % 2 == 0)
        }
        Source(msgsIn).runWith(jmsSink)

        val jmsSource = JmsConsumer.ackSource(
          JmsConsumerSettings(consumerConfig, connectionFactory)
            .withSessionCount(5)
            .withQueue("numbers")
            .withSelector("IsOdd = TRUE")
        )

        val oddMsgsIn = msgsIn.filter(msg => msg.body.toInt % 2 == 1)
        val result = jmsSource
          .take(oddMsgsIn.size)
          .map { env =>
            env.acknowledge()
            env.message
          }
          .runWith(Sink.seq)
        // We should have only received the odd numbers in the list

        val sortedResult = result.futureValue.sortBy(msg => msg.getIntProperty("Number"))
        forAll(sortedResult.zip(oddMsgsIn)) {
          case (out, in) =>
            out.getIntProperty("Number") shouldEqual in.properties("Number")
            out.getBooleanProperty("IsOdd") shouldEqual in.properties("IsOdd")
            out.getBooleanProperty("IsEven") shouldEqual in.properties("IsEven")
            // Make sure we are only receiving odd numbers
            out.getIntProperty("Number") % 2 shouldEqual 1
        }
    }

    "applying backpressure when the consumer is slower than the producer" in withConnectionFactory() {
      connectionFactory =>
        val queueName = createName("test")
        val in = 0 to 25 map (i => ('a' + i).asInstanceOf[Char].toString)
        Source(in).runWith(
          JmsProducer.textSink(JmsProducerSettings(producerConfig, connectionFactory).withQueue(queueName))
        )

        val jmsSource: Source[AckEnvelope, JmsConsumerControl] = JmsConsumer.ackSource(
          JmsConsumerSettings(consumerConfig, connectionFactory).withSessionCount(5).withQueue(queueName)
        )

        val result = jmsSource
          .throttle(10, 1.second, 1, ThrottleMode.shaping)
          .take(in.size)
          .map(env => (env, env.message.asInstanceOf[TextMessage].getText))
          .map { case (env, text) => env.acknowledge(); text }
          .runWith(Sink.seq)

        result.futureValue should contain theSameElementsAs in
    }

    "disconnection should fail the stage after exhausting retries" in withServer() { server =>
      val connectionFactory = server.createConnectionFactory
      val result = JmsConsumer
        .ackSource(
          JmsConsumerSettings(consumerConfig, connectionFactory)
            .withQueue("test")
            .withConnectionRetrySettings(ConnectionRetrySettings(system).withMaxRetries(3))
        )
        .runWith(Sink.seq)
      Thread.sleep(500)
      server.stop()
      val ex = result.failed.futureValue
      ex shouldBe a[ConnectionRetryException]
      ex.getCause shouldBe a[JMSException]
    }

    "publish and consume elements through a topic " in withConnectionFactory() { connectionFactory =>
      import system.dispatcher

      val topicName = createName("topic")
      val jmsTopicSink: Sink[String, Future[Done]] = JmsProducer.textSink(
        JmsProducerSettings(producerConfig, connectionFactory).withTopic(topicName)
      )
      val jmsTopicSink2: Sink[String, Future[Done]] = JmsProducer.textSink(
        JmsProducerSettings(producerConfig, connectionFactory).withTopic(topicName)
      )

      val in = List("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k")
      val inNumbers = (1 to 10).map(_.toString)

      val jmsTopicSource: Source[AckEnvelope, JmsConsumerControl] = JmsConsumer.ackSource(
        JmsConsumerSettings(consumerConfig, connectionFactory).withSessionCount(1).withTopic(topicName)
      )
      val jmsSource2: Source[AckEnvelope, JmsConsumerControl] = JmsConsumer.ackSource(
        JmsConsumerSettings(consumerConfig, connectionFactory).withSessionCount(1).withTopic(topicName)
      )

      val expectedSize = in.size + inNumbers.size
      val result1 = jmsTopicSource
        .take(expectedSize)
        .map(env => (env, env.message.asInstanceOf[TextMessage].getText))
        .map { case (env, text) => env.acknowledge(); text }
        .runWith(Sink.seq)
        .map(_.sorted)
      val result2 = jmsSource2
        .take(expectedSize)
        .map(env => (env, env.message.asInstanceOf[TextMessage].getText))
        .map { case (env, text) => env.acknowledge(); text }
        .runWith(Sink.seq)
        .map(_.sorted)

      //We wait a little to be sure that the source is connected
      Thread.sleep(500)

      Source(in).runWith(jmsTopicSink)
      Source(inNumbers).runWith(jmsTopicSink2)

      val expectedList: List[String] = in ++ inNumbers
      result1.futureValue should contain theSameElementsAs expectedList
      result2.futureValue should contain theSameElementsAs expectedList
    }

    "ensure no message loss when stopping a stream" in withConnectionFactory() { connectionFactory =>
      val queueName = createName("numbers")
      val jmsSink: Sink[JmsTextMessage, Future[Done]] = JmsProducer.sink(
        JmsProducerSettings(producerConfig, connectionFactory).withQueue(queueName)
      )

      val (publishKillSwitch, publishedData) = Source
        .unfold(1)(n => Some(n + 1 -> n))
        .throttle(15, 1.second, 2, ThrottleMode.shaping) // Higher than consumption rate.
        .viaMat(KillSwitches.single)(Keep.right)
        .alsoTo(Flow[Int].map(n => JmsTextMessage(n.toString).withProperty("Number", n)).to(jmsSink))
        .toMat(Sink.seq)(Keep.both)
        .run()

      val jmsSource: Source[AckEnvelope, JmsConsumerControl] = JmsConsumer.ackSource(
        JmsConsumerSettings(consumerConfig, connectionFactory)
          .withSessionCount(5)
          .withQueue(queueName)
      )

      val resultQueue = new LinkedBlockingQueue[String]()

      val (killSwitch, streamDone) = jmsSource
        .throttle(10, 1.second, 2, ThrottleMode.shaping)
        .toMat(
          Sink.foreach { env =>
            resultQueue.add(env.message.asInstanceOf[TextMessage].getText)
            env.acknowledge()
          }
        )(Keep.both)
        .run()

      // Need to wait for the stream to have started and running for sometime
      Thread.sleep(2000)

      killSwitch.shutdown()

      streamDone.futureValue shouldBe Done

      // Keep publishing for another 2 seconds to make sure we killed the consumption mid-stream.
      Thread.sleep(2000)

      publishKillSwitch.shutdown()
      val numsIn = publishedData.futureValue

      resultQueue.size should be < numsIn.size

      val killSwitch2 = jmsSource
        .to(
          Sink.foreach { env =>
            resultQueue.add(env.message.asInstanceOf[TextMessage].getText)
            env.acknowledge()
          }
        )
        .run()

      val resultList = new mutable.ArrayBuffer[String](numsIn.size)

      @tailrec
      def keepPolling(): Unit =
        Option(resultQueue.poll(2, TimeUnit.SECONDS)) match {
          case Some(entry) =>
            resultList += entry
            keepPolling()
          case None =>
        }

      keepPolling()

      killSwitch2.shutdown()

      resultList.toSet should contain.theSameElementsAs(numsIn.map(_.toString))
    }

    "ensure no message loss when aborting a stream" in withConnectionFactory() { connectionFactory =>
      val queueName = createName("numbers")
      val jmsSink: Sink[JmsTextMessage, Future[Done]] = JmsProducer.sink(
        JmsProducerSettings(producerConfig, connectionFactory).withQueue(queueName)
      )

      val (publishKillSwitch, publishedData) = Source
        .unfold(1)(n => Some(n + 1 -> n))
        .throttle(15, 1.second, 2, ThrottleMode.shaping) // Higher than consumption rate.
        .viaMat(KillSwitches.single)(Keep.right)
        .alsoTo(Flow[Int].map(n => JmsTextMessage(n.toString).withProperty("Number", n)).to(jmsSink))
        .toMat(Sink.seq)(Keep.both)
        .run()

      // We need this ack mode for AMQ to not lose messages as ack normally acks any messages read on the session.
      val individualAck = new AcknowledgeMode(ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE)

      val jmsSource: Source[AckEnvelope, JmsConsumerControl] = JmsConsumer.ackSource(
        JmsConsumerSettings(consumerConfig, connectionFactory)
          .withSessionCount(5)
          .withQueue(queueName)
          .withAcknowledgeMode(individualAck)
      )

      val resultQueue = new LinkedBlockingQueue[String]()

      val (killSwitch, streamDone) = jmsSource
        .throttle(10, 1.second, 2, ThrottleMode.shaping)
        .toMat(
          Sink.foreach { env =>
            resultQueue.add(env.message.asInstanceOf[TextMessage].getText)
            env.acknowledge()
          }
        )(Keep.both)
        .run()

      // Need to wait for the stream to have started and running for sometime.
      Thread.sleep(2000)

      val ex = new Exception("Test exception")
      killSwitch.abort(ex)

      import scala.concurrent.ExecutionContext.Implicits.global

      val resultTry = streamDone.map(Success(_)).recover { case e => Failure(e) }.futureValue

      // Keep publishing for another 2 seconds to make sure we killed the consumption mid-stream.
      Thread.sleep(2000)

      publishKillSwitch.shutdown()
      val numsIn = publishedData.futureValue

      // Ensure we break the stream while reading, not all input should have been read.
      resultQueue.size should be < numsIn.size
      resultTry shouldBe Failure(ex)

      val killSwitch2 = jmsSource
        .to(
          Sink.foreach { env =>
            resultQueue.add(env.message.asInstanceOf[TextMessage].getText)
            env.acknowledge()
          }
        )
        .run()

      val resultList = new mutable.ArrayBuffer[String](numsIn.size)

      @tailrec
      def keepPolling(): Unit =
        Option(resultQueue.poll(2, TimeUnit.SECONDS)) match {
          case Some(entry) =>
            resultList += entry
            keepPolling()
          case None =>
        }

      keepPolling()

      killSwitch2.shutdown()

      resultList.toSet should contain.theSameElementsAs(numsIn.map(_.toString))
    }

    "send acknowledgments back to the broker after max.ack.interval" in withConnectionFactory() { connectionFactory =>
      val testQueue = "test"
      val aMessage = "message"
      val maxAckInterval = 1.second
      Source
        .single(aMessage)
        .runWith(JmsProducer.textSink(JmsProducerSettings(producerConfig, connectionFactory).withQueue(testQueue)))
      val source = JmsConsumer.ackSource(
        JmsConsumerSettings(system, connectionFactory)
          .withMaxPendingAcks(100)
          .withMaxAckInterval(maxAckInterval)
          .withQueue(testQueue)
      )

      val (consumerControl, probe) = source
        .map { env =>
          env.acknowledge()
          env.message match {
            case message: TextMessage => Some(message.getText)
            case _ => None

          }
        }
        .toMat(TestSink())(Keep.both)
        .run()

      probe.requestNext(convertSpanToDuration(patienceConfig.timeout)) shouldBe Some(aMessage)

      eventually {
        isQueueEmpty(testQueue) shouldBe true
      }

      consumerControl.shutdown()
      probe.expectComplete()

      // Consuming again should give us no elements, as msg was acked and therefore removed from the broker
      val (emptyConsumerControl, emptySourceProbe) = source.toMat(TestSink())(Keep.both).run()
      emptySourceProbe.ensureSubscription().expectNoMessage()
      emptyConsumerControl.shutdown()
      emptySourceProbe.expectComplete()
    }
  }
}
