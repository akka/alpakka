/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingQueue, TimeUnit}

import akka.Done
import akka.stream._
import akka.stream.alpakka.jms._
import akka.stream.alpakka.jms.scaladsl.{JmsConsumer, JmsConsumerControl, JmsProducer}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import javax.jms.{JMSException, TextMessage}
import org.scalatest.Inspectors._

import scala.annotation.tailrec
import scala.collection.{immutable, mutable}
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

class JmsTxConnectorsSpec extends JmsSpec {

  override implicit val patienceConfig = PatienceConfig(2.minutes)

  "The JMS Transactional Connectors" should {
    "publish and consume strings through a queue" in withConnectionFactory() { connectionFactory =>
      val jmsSink: Sink[String, Future[Done]] = JmsProducer.textSink(
        JmsProducerSettings(producerConfig, connectionFactory).withQueue("test")
      )

      val in = 0 to 25 map (i => ('a' + i).asInstanceOf[Char].toString)
      Source(in).runWith(jmsSink)

      val jmsSource: Source[TxEnvelope, JmsConsumerControl] = JmsConsumer.txSource(
        JmsConsumerSettings(consumerConfig, connectionFactory).withSessionCount(5).withQueue("test")
      )

      val result = jmsSource
        .take(in.size)
        .map(env => (env, env.message.asInstanceOf[TextMessage].getText))
        .map { case (env, text) => env.commit(); text }
        .runWith(Sink.seq)

      result.futureValue should contain theSameElementsAs in
    }

    "publish and consume JMS text messages with properties through a queue" in withConnectionFactory() {
      connectionFactory =>
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

        //#source
        val jmsSource: Source[TxEnvelope, JmsConsumerControl] = JmsConsumer.txSource(
          JmsConsumerSettings(consumerConfig, connectionFactory)
            .withSessionCount(5)
            .withAckTimeout(1.second)
            .withQueue("numbers")
        )

        val result: Future[immutable.Seq[javax.jms.Message]] =
          jmsSource
            .take(msgsIn.size)
            .map { txEnvelope =>
              txEnvelope.commit()
              txEnvelope.message
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

    "ensure re-delivery when rollback JMS text messages through a queue" in withConnectionFactory() {
      connectionFactory =>
        val jmsSink: Sink[JmsTextMessage, Future[Done]] = JmsProducer.sink(
          JmsProducerSettings(producerConfig, connectionFactory).withQueue("numbers")
        )
        val msgsIn = 1 to 100 map { n =>
          JmsTextMessage(n.toString).withProperty("Number", n)
        }

        Source(msgsIn).runWith(jmsSink)

        val jmsSource: Source[TxEnvelope, JmsConsumerControl] = JmsConsumer.txSource(
          JmsConsumerSettings(consumerConfig, connectionFactory).withSessionCount(5).withQueue("numbers")
        )

        val expectedElements = (1 to 100) ++ (2 to 100 by 2) map (_.toString)

        val rolledBackSet = ConcurrentHashMap.newKeySet[Int]()

        val result = jmsSource
          .take(expectedElements.size)
          .map { env =>
            val id = env.message.getIntProperty("Number")
            if (id % 2 == 0 && !rolledBackSet.contains(id)) {
              rolledBackSet.add(id)
              env.rollback()
            } else {
              env.commit()
            }
            env.message.asInstanceOf[TextMessage].getText
          }
          .runWith(Sink.seq)

        result.futureValue should contain theSameElementsAs expectedElements
    }

    "publish JMS text messages with properties through a queue and consume them with a selector" in withConnectionFactory() {
      connectionFactory =>
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

        val jmsSource = JmsConsumer.txSource(
          JmsConsumerSettings(consumerConfig, connectionFactory)
            .withSessionCount(5)
            .withQueue("numbers")
            .withSelector("IsOdd = TRUE")
        )

        val oddMsgsIn = msgsIn.filter(msg => msg.body.toInt % 2 == 1)
        val result = jmsSource
          .take(oddMsgsIn.size)
          .map { env =>
            env.commit()
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
        val in = 0 to 25 map (i => ('a' + i).asInstanceOf[Char].toString)
        Source(in).runWith(
          JmsProducer.textSink(JmsProducerSettings(producerConfig, connectionFactory).withQueue("test"))
        )

        val jmsSource: Source[TxEnvelope, JmsConsumerControl] = JmsConsumer.txSource(
          JmsConsumerSettings(consumerConfig, connectionFactory).withSessionCount(5).withQueue("test")
        )

        val result = jmsSource
          .throttle(10, 1.second, 1, ThrottleMode.shaping)
          .take(in.size)
          .map(env => (env, env.message.asInstanceOf[TextMessage].getText))
          .map { case (env, text) => env.commit(); text }
          .runWith(Sink.seq)

        result.futureValue should contain theSameElementsAs in
    }

    "disconnection should fail the stage after exhausting retries" in withServer() { server =>
      val connectionFactory = server.createConnectionFactory
      val result = JmsConsumer
        .txSource(
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

      val jmsTopicSink: Sink[String, Future[Done]] = JmsProducer.textSink(
        JmsProducerSettings(producerConfig, connectionFactory).withTopic("topic")
      )
      val jmsTopicSink2: Sink[String, Future[Done]] = JmsProducer.textSink(
        JmsProducerSettings(producerConfig, connectionFactory).withTopic("topic")
      )

      val in = List("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k")
      val inNumbers = (1 to 10).map(_.toString)

      val jmsTopicSource: Source[TxEnvelope, JmsConsumerControl] = JmsConsumer.txSource(
        JmsConsumerSettings(consumerConfig, connectionFactory).withSessionCount(1).withTopic("topic")
      )
      val jmsSource2: Source[TxEnvelope, JmsConsumerControl] = JmsConsumer.txSource(
        JmsConsumerSettings(consumerConfig, connectionFactory).withSessionCount(1).withTopic("topic")
      )

      val expectedSize = in.size + inNumbers.size
      val result1 = jmsTopicSource
        .take(expectedSize)
        .map(env => (env, env.message.asInstanceOf[TextMessage].getText))
        .map { case (env, text) => env.commit(); text }
        .runWith(Sink.seq)
        .map(_.sorted)
      val result2 = jmsSource2
        .take(expectedSize)
        .map(env => (env, env.message.asInstanceOf[TextMessage].getText))
        .map { case (env, text) => env.commit(); text }
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
      val jmsSink: Sink[JmsTextMessage, Future[Done]] = JmsProducer.sink(
        JmsProducerSettings(producerConfig, connectionFactory).withQueue("numbers")
      )

      val (publishKillSwitch, publishedData) = Source
        .unfold(1)(n => Some(n + 1 -> n))
        .throttle(15, 1.second, 2, ThrottleMode.shaping) // Higher than consumption rate.
        .viaMat(KillSwitches.single)(Keep.right)
        .alsoTo(Flow[Int].map(n => JmsTextMessage(n.toString).withProperty("Number", n)).to(jmsSink))
        .toMat(Sink.seq)(Keep.both)
        .run()

      val jmsSource: Source[TxEnvelope, JmsConsumerControl] = JmsConsumer.txSource(
        JmsConsumerSettings(consumerConfig, connectionFactory).withSessionCount(5).withQueue("numbers")
      )

      val resultQueue = new LinkedBlockingQueue[String]()

      val (killSwitch, streamDone) = jmsSource
        .throttle(10, 1.second, 2, ThrottleMode.shaping)
        .toMat(
          Sink.foreach { env =>
            resultQueue.add(env.message.asInstanceOf[TextMessage].getText)
            env.commit()
          }
        )(Keep.both)
        .run()

      // Need to wait for the stream to have started and running for sometime.
      Thread.sleep(1000)

      killSwitch.shutdown()

      streamDone.futureValue shouldBe Done

      // Keep publishing for another 2 seconds to make sure we killed the consumption mid-stream.
      Thread.sleep(2000)

      publishKillSwitch.shutdown()
      val numsIn = publishedData.futureValue

      // Ensure we break the stream while reading, not all input should have been read.
      resultQueue.size should be < numsIn.size

      val killSwitch2 = jmsSource
        .to(
          Sink.foreach { env =>
            resultQueue.add(env.message.asInstanceOf[TextMessage].getText)
            env.commit()
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

      // messages might get delivered more than once, use set to ignore duplicates
      resultList.toSet should contain theSameElementsAs numsIn.map(_.toString)
    }

    "ensure no message loss when aborting a stream" in withConnectionFactory() { connectionFactory =>
      val jmsSink: Sink[JmsTextMessage, Future[Done]] = JmsProducer.sink(
        JmsProducerSettings(producerConfig, connectionFactory).withQueue("numbers")
      )

      val (publishKillSwitch, publishedData) = Source
        .unfold(1)(n => Some(n + 1 -> n))
        .throttle(15, 1.second, 2, ThrottleMode.shaping) // Higher than consumption rate.
        .viaMat(KillSwitches.single)(Keep.right)
        .alsoTo(Flow[Int].map(n => JmsTextMessage(n.toString).withProperty("Number", n)).to(jmsSink))
        .toMat(Sink.seq)(Keep.both)
        .run()

      val jmsSource: Source[TxEnvelope, JmsConsumerControl] = JmsConsumer.txSource(
        JmsConsumerSettings(consumerConfig, connectionFactory).withSessionCount(5).withQueue("numbers")
      )

      val resultQueue = new LinkedBlockingQueue[String]()

      val (killSwitch, streamDone) = jmsSource
        .throttle(10, 1.second, 2, ThrottleMode.shaping)
        .toMat(
          Sink.foreach { env =>
            resultQueue.add(env.message.asInstanceOf[TextMessage].getText)
            env.commit()
          }
        )(Keep.both)
        .run()

      // Need to wait for the stream to have started and running for sometime.
      Thread.sleep(2000)

      val ex = new Exception("Test exception")
      killSwitch.abort(ex)

      import system.dispatcher
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
            env.commit()
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

      // messages might get delivered more than once, use set to ignore duplicates
      resultList.toSet should contain theSameElementsAs numsIn.map(_.toString)
    }

    "ensure no message loss or starvation when exceptions occur in a stream missing commits" in withConnectionFactory() {
      connectionFactory =>
        val jmsSink: Sink[JmsTextMessage, Future[Done]] = JmsProducer.sink(
          JmsProducerSettings(producerConfig, connectionFactory).withQueue("numbers")
        )

        val (publishKillSwitch, publishedData) = Source
          .unfold(1)(n => Some(n + 1 -> n))
          .throttle(15, 1.second, 2, ThrottleMode.shaping) // Higher than consumption rate.
          .viaMat(KillSwitches.single)(Keep.right)
          .alsoTo(Flow[Int].map(n => JmsTextMessage(n.toString).withProperty("Number", n)).to(jmsSink))
          .toMat(Sink.seq)(Keep.both)
          .run()

        val jmsSource: Source[TxEnvelope, JmsConsumerControl] = JmsConsumer.txSource(
          JmsConsumerSettings(consumerConfig, connectionFactory)
            .withSessionCount(5)
            .withQueue("numbers")
            .withAckTimeout(10.millis)
        )

        val resultQueue = new LinkedBlockingQueue[String]()

        val r = new java.util.Random

        val thisDecider: Supervision.Decider = {
          case ex =>
            Supervision.resume
        }

        val (killSwitch, streamDone) = jmsSource
          .throttle(10, 1.second, 2, ThrottleMode.shaping)
          .map { env =>
            val text = env.message.asInstanceOf[TextMessage].getText
            if (r.nextInt(3) <= 1) throw new IllegalStateException(s"Test Exception on $text")
            resultQueue.add(text)
            env.commit()
            1
          }
          .recover {
            case _: Throwable => 1
          }
          .withAttributes(ActorAttributes.supervisionStrategy(thisDecider))
          .toMat(Sink.ignore)(Keep.both)
          .run()

        // Need to wait for the stream to have started and running for sometime.
        Thread.sleep(3000)

        killSwitch.shutdown()

        streamDone.futureValue shouldBe Done

        // Keep publishing for another 2 seconds to make sure we killed the consumption mid-stream.
        Thread.sleep(2000)

        publishKillSwitch.shutdown()
        val numsIn = publishedData.futureValue

        // Ensure we break the stream while reading, not all input should have been read.
        resultQueue.size should be < numsIn.size

        val killSwitch2 = jmsSource
          .to(
            Sink.foreach { env =>
              resultQueue.add(env.message.asInstanceOf[TextMessage].getText)
              env.commit()
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

        // messages might get delivered more than once, use set to ignore duplicates
        resultList.toSet should contain theSameElementsAs numsIn.map(_.toString)
    }

    "ensure no message loss or starvation when timeouts occur in a stream processing" in withConnectionFactory() {
      connectionFactory =>
        val jmsSink: Sink[JmsTextMessage, Future[Done]] = JmsProducer.sink(
          JmsProducerSettings(producerConfig, connectionFactory).withQueue("numbers")
        )

        val (publishKillSwitch, publishedData) = Source
          .unfold(1)(n => Some(n + 1 -> n))
          .throttle(15, 1.second, 2, ThrottleMode.shaping) // Higher than consumption rate.
          .viaMat(KillSwitches.single)(Keep.right)
          .alsoTo(Flow[Int].map(n => JmsTextMessage(n.toString).withProperty("Number", n)).to(jmsSink))
          .toMat(Sink.seq)(Keep.both)
          .run()

        val jmsSource: Source[TxEnvelope, JmsConsumerControl] = JmsConsumer.txSource(
          JmsConsumerSettings(consumerConfig, connectionFactory)
            .withSessionCount(5)
            .withQueue("numbers")
            .withAckTimeout(10.millis)
        )

        val resultQueue = new LinkedBlockingQueue[String]()

        val r = new java.util.Random

        val (killSwitch, streamDone) = jmsSource
          .throttle(10, 1.second, 2, ThrottleMode.shaping)
          .toMat(
            Sink.foreach { env =>
              val text = env.message.asInstanceOf[TextMessage].getText
              if (r.nextInt(3) <= 1) {
                // Artifially timing out this message
                Thread.sleep(20)
              }
              resultQueue.add(text)
              env.commit()
            }
          )(Keep.both)
          .run()

        // Need to wait for the stream to have started and running for sometime.
        Thread.sleep(3000)

        killSwitch.shutdown()

        streamDone.futureValue shouldBe Done

        // Keep publishing for another 2 seconds to make sure we killed the consumption mid-stream.
        Thread.sleep(2000)

        publishKillSwitch.shutdown()
        val numsIn = publishedData.futureValue

        // Ensure we break the stream while reading, not all input should have been read.
        resultQueue.size should be < numsIn.size

        val killSwitch2 = jmsSource
          .to(
            Sink.foreach { env =>
              resultQueue.add(env.message.asInstanceOf[TextMessage].getText)
              env.commit()
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

        // messages might get delivered more than once, use set to ignore duplicates
        resultList.toSet should contain theSameElementsAs numsIn.map(_.toString)
    }
  }
}
