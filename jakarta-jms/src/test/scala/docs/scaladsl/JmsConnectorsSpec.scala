/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import java.nio.charset.Charset
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{CountDownLatch, LinkedBlockingQueue, ThreadLocalRandom, TimeUnit}

import akka.stream._
import akka.stream.alpakka.jakartajms._
import akka.stream.alpakka.jakartajms.scaladsl._
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.{Done, NotUsed}
import jakarta.jms._

import org.apache.activemq.command.ActiveMQQueue
import org.apache.activemq.{ActiveMQConnectionFactory, ActiveMQSession}
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

final case class DummyObject(payload: String)

class JmsConnectorsSpec extends JmsSpec {

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(2.minutes)

  "The JMS Connectors" should {
    "publish and consume strings through a queue" in withServer() { server =>
      val url = server.brokerUri
      //#connection-factory
      //#text-sink
      //#text-source
      val connectionFactory: jakarta.jms.ConnectionFactory = new org.apache.activemq.ActiveMQConnectionFactory(url)
      //#connection-factory
      //#text-sink
      //#text-source

      //#text-sink

      val jmsSink: Sink[String, Future[Done]] = JmsProducer.textSink(
        JmsProducerSettings(system, connectionFactory).withQueue("test")
      )

      val in = List("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k")
      val streamCompletion: Future[Done] =
        Source(in)
          .runWith(jmsSink)
      //#text-sink

      //#text-source
      val jmsSource: Source[String, JmsConsumerControl] = JmsConsumer.textSource(
        JmsConsumerSettings(system, connectionFactory).withQueue("test")
      )

      val result: Future[immutable.Seq[String]] = jmsSource.take(in.size).runWith(Sink.seq)
      //#text-source

      streamCompletion.futureValue shouldEqual Done
      result.futureValue shouldEqual in
    }

    "publish and consume serializable objects through a queue" in withConnectionFactory() { connFactory =>
      //#object-sink
      //#object-source
      val connectionFactory = connFactory.asInstanceOf[ActiveMQConnectionFactory]
      connectionFactory.setTrustedPackages(List(classOf[DummyObject].getPackage.getName).asJava)
      //#object-sink
      //#object-source

      //#object-sink

      val jmsSink: Sink[Serializable, Future[Done]] = JmsProducer.objectSink(
        JmsProducerSettings(system, connectionFactory).withQueue("test")
      )
      val in = DummyObject("ThisIsATest")
      val streamCompletion: Future[Done] =
        Source
          .single(in)
          .runWith(jmsSink)
      //#object-sink

      //#object-source
      val jmsSource: Source[java.io.Serializable, JmsConsumerControl] = JmsConsumer.objectSource(
        JmsConsumerSettings(system, connectionFactory).withQueue("test")
      )

      val result: Future[java.io.Serializable] =
        jmsSource
          .take(1)
          .runWith(Sink.head)
      //#object-source

      streamCompletion.futureValue shouldEqual Done
      result.futureValue shouldEqual in
    }

    "publish and consume bytearray through a queue" in withConnectionFactory() { connectionFactory =>
      //#bytearray-sink
      val jmsSink: Sink[Array[Byte], Future[Done]] = JmsProducer.bytesSink(
        JmsProducerSettings(system, connectionFactory).withQueue("test")
      )
      val in: Array[Byte] = "ThisIsATest".getBytes(Charset.forName("UTF-8"))
      val streamCompletion: Future[Done] =
        Source
          .single(in)
          .runWith(jmsSink)
      //#bytearray-sink

      //#bytearray-source
      val jmsSource: Source[Array[Byte], JmsConsumerControl] = JmsConsumer.bytesSource(
        JmsConsumerSettings(system, connectionFactory).withQueue("test")
      )

      val result: Future[Array[Byte]] =
        jmsSource
          .take(1)
          .runWith(Sink.head)
      //#bytearray-source

      streamCompletion.futureValue shouldEqual Done
      result.futureValue shouldEqual in
    }

    "publish and consume map through a queue" in withConnectionFactory() { connectionFactory =>
      //#map-sink
      val jmsSink: Sink[Map[String, Any], Future[Done]] = JmsProducer.mapSink(
        JmsProducerSettings(system, connectionFactory).withQueue("test")
      )

      val input = List(
        Map[String, Any](
          "string" -> "value",
          "int value" -> 42,
          "double value" -> 43.toDouble,
          "short value" -> 7.toShort,
          "boolean value" -> true,
          "long value" -> 7.toLong,
          "bytearray" -> "AStringAsByteArray".getBytes(Charset.forName("UTF-8")),
          "byte" -> 1.toByte
        )
      )

      val streamCompletion: Future[Done] =
        Source(input)
          .runWith(jmsSink)
      //#map-sink

      //#map-source
      val jmsSource: Source[Map[String, Any], JmsConsumerControl] = JmsConsumer.mapSource(
        JmsConsumerSettings(system, connectionFactory).withQueue("test")
      )

      val result: Future[immutable.Seq[Map[String, Any]]] =
        jmsSource
          .take(1)
          .runWith(Sink.seq)
      //#map-source

      streamCompletion.futureValue shouldEqual Done
      result.futureValue.zip(input).foreach {
        case (out, in) =>
          out("string") shouldEqual in("string")
          out("int value") shouldEqual in("int value")
          out("double value") shouldEqual in("double value")
          out("short value") shouldEqual in("short value")
          out("boolean value") shouldEqual in("boolean value")
          out("long value") shouldEqual in("long value")
          out("byte") shouldEqual in("byte")

          val outBytes = out("bytearray").asInstanceOf[Array[Byte]]
          new String(outBytes, Charset.forName("UTF-8")) shouldBe "AStringAsByteArray"
      }
    }

    "publish and consume JMS text messages with properties through a queue" in withConnectionFactory() {
      connectionFactory =>
        val jmsSink: Sink[JmsTextMessage, Future[Done]] = JmsProducer.sink(
          JmsProducerSettings(producerConfig, connectionFactory).withQueue("numbers")
        )

        //#create-messages-with-properties
        val msgsIn = (1 to 10).toList.map { n =>
          akka.stream.alpakka.jakartajms
            .JmsTextMessage(n.toString)
            .withProperty("Number", n)
            .withProperty("IsOdd", n % 2 == 1)
            .withProperty("IsEven", n % 2 == 0)
        }
        //#create-messages-with-properties

        Source(msgsIn).runWith(jmsSink)

        //#jms-source
        val jmsSource: Source[jakarta.jms.Message, JmsConsumerControl] = JmsConsumer(
          JmsConsumerSettings(system, connectionFactory).withQueue("numbers")
        )

        val (control, result): (JmsConsumerControl, Future[immutable.Seq[String]]) =
          jmsSource
            .take(msgsIn.size)
            .map {
              case t: jakarta.jms.TextMessage => t.getText
              case other => sys.error(s"unexpected message type ${other.getClass}")
            }
            .toMat(Sink.seq)(Keep.both)
            .run()
        //#jms-source

        result.futureValue should have size 10
        //#jms-source

        control.shutdown()
      //#jms-source
    }

    "publish and consume JMS text messages" in withConnectionFactory() { connectionFactory =>
      //#create-jms-sink
      val jmsSink: Sink[JmsTextMessage, Future[Done]] = JmsProducer.sink(
        JmsProducerSettings(producerConfig, connectionFactory).withQueue("numbers")
      )

      val finished: Future[Done] =
        Source(immutable.Seq("Message A", "Message B"))
          .map(JmsTextMessage(_))
          .runWith(jmsSink)
      //#create-jms-sink

      val jmsSource: Source[Message, JmsConsumerControl] = JmsConsumer(
        JmsConsumerSettings(consumerConfig, connectionFactory).withQueue("numbers")
      )

      val result: Future[Seq[Message]] = jmsSource.take(2).runWith(Sink.seq)

      finished.futureValue shouldBe Done

      result.futureValue.zip(immutable.Seq("Message A", "Message B")).foreach {
        case (out, in) =>
          out.asInstanceOf[TextMessage].getText shouldEqual in
      }
    }

    "publish and consume JMS text messages with header through a queue" in withConnectionFactory() {
      connectionFactory =>
        val jmsSink: Sink[JmsTextMessage, Future[Done]] = JmsProducer.sink(
          JmsProducerSettings(producerConfig, connectionFactory).withQueue("numbers")
        )

        //#create-messages-with-headers
        val msgsIn = (1 to 10).toList.map { n =>
          JmsTextMessage(n.toString)
            .withHeader(JmsType("type"))
            .withHeader(JmsCorrelationId("correlationId"))
            .withHeader(JmsReplyTo.queue("test-reply"))
            .withHeader(JmsTimeToLive(FiniteDuration(999, TimeUnit.SECONDS)))
            .withHeader(JmsPriority(2))
            .withHeader(JmsDeliveryMode(DeliveryMode.NON_PERSISTENT))
        }
        //#create-messages-with-headers

        Source(msgsIn).runWith(jmsSink)

        val jmsSource: Source[Message, JmsConsumerControl] = JmsConsumer(
          JmsConsumerSettings(consumerConfig, connectionFactory).withQueue("numbers")
        )

        val result: Future[Seq[Message]] = jmsSource.take(msgsIn.size).runWith(Sink.seq)

        // The sent message and the receiving one should have the same properties
        result.futureValue.foreach { outMsg =>
          outMsg.getJMSType shouldBe "type"
          outMsg.getJMSCorrelationID shouldBe "correlationId"
          outMsg.getJMSReplyTo.asInstanceOf[ActiveMQQueue].getQueueName shouldBe "test-reply"
          outMsg.getJMSExpiration should not be 0
          outMsg.getJMSPriority shouldBe 2
          outMsg.getJMSDeliveryMode shouldBe DeliveryMode.NON_PERSISTENT
        }
    }

    "publish and consume JMS text messages through a queue with custom queue creator " in withConnectionFactory() {
      connectionFactory =>
        //#custom-destination
        def createQueue(destinationName: String): Session => jakarta.jms.Queue = { (session: Session) =>
          val amqSession = session.asInstanceOf[ActiveMQSession]
          amqSession.createQueue(s"my-$destinationName")
        }
        //#custom-destination

        val jmsSink: Sink[JmsTextMessage, Future[Done]] = JmsProducer.sink(
          JmsProducerSettings(producerConfig, connectionFactory)
            .withDestination(CustomDestination("custom", createQueue("custom")))
        )

        val msgsIn: immutable.Seq[JmsTextMessage] = (1 to 10).toList.map { n =>
          JmsTextMessage(n.toString)
        }

        Source(msgsIn).runWith(jmsSink)

        //#custom-destination

        val jmsSource: Source[jakarta.jms.Message, JmsConsumerControl] = JmsConsumer(
          JmsConsumerSettings(consumerConfig, connectionFactory)
            .withDestination(CustomDestination("custom", createQueue("custom")))
        )
        //#custom-destination

        val result: Future[immutable.Seq[jakarta.jms.Message]] = jmsSource.take(msgsIn.size).runWith(Sink.seq)

        // The sent message and the receiving one should have the same properties
        result.futureValue.zip(msgsIn).foreach {
          case (out, in) =>
            out.asInstanceOf[TextMessage].getText shouldEqual in.body
        }
    }

    "publish JMS text messages with properties through a queue and consume them with a selector" in withConnectionFactory() {
      connectionFactory =>
        val jmsSink: Sink[JmsTextMessage, Future[Done]] = JmsProducer.sink(
          JmsProducerSettings(producerConfig, connectionFactory).withQueue("numbers")
        )

        val msgsIn = (1 to 10).toList.map { n =>
          JmsTextMessage(n.toString)
            .withProperty("Number", n)
            .withProperty("IsOdd", n % 2 == 1)
            .withProperty("IsEven", n % 2 == 0)
        }
        Source(msgsIn).runWith(jmsSink)

        //#source-with-selector
        val jmsSource = JmsConsumer(
          JmsConsumerSettings(consumerConfig, connectionFactory)
            .withQueue("numbers")
            .withSelector("IsOdd = TRUE")
        )
        //#source-with-selector

        val oddMsgsIn = msgsIn.filter(msg => msg.body.toInt % 2 == 1)
        val result = jmsSource.take(oddMsgsIn.size).runWith(Sink.seq)

        // We should have only received the odd numbers in the list
        result.futureValue.zip(oddMsgsIn).foreach {
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
        val in = List("a", "b", "c")
        Source(in).runWith(
          JmsProducer.textSink(JmsProducerSettings(producerConfig, connectionFactory).withQueue("test"))
        )

        val result = JmsConsumer
          .textSource(JmsConsumerSettings(consumerConfig, connectionFactory).withBufferSize(1).withQueue("test"))
          .throttle(1, 1.second, 1, ThrottleMode.shaping)
          .take(in.size)
          .runWith(Sink.seq)

        result.futureValue shouldEqual in
    }

    "disconnection should fail the stage after exhausting retries" in withServer() { server =>
      val connectionFactory = server.createConnectionFactory
      val result = JmsConsumer(
        JmsConsumerSettings(consumerConfig, connectionFactory)
          .withQueue("test")
          .withConnectionRetrySettings(ConnectionRetrySettings(system).withMaxRetries(3))
      ).runWith(Sink.seq)
      Thread.sleep(500)
      server.stop()
      val ex = result.failed.futureValue
      ex shouldBe a[ConnectionRetryException]
      ex.getCause shouldBe a[JMSException]
    }

    "publish and consume elements through a topic with custom topic creator" in withConnectionFactory() {
      connectionFactory =>
        def createTopic(destinationName: String): Session => jakarta.jms.Topic = { (session: Session) =>
          val amqSession = session.asInstanceOf[ActiveMQSession]
          amqSession.createTopic(s"my-$destinationName")
        }

        //#create-custom-jms-topic-sink
        val jmsTopicSink: Sink[String, Future[Done]] = JmsProducer.textSink(
          JmsProducerSettings(producerConfig, connectionFactory)
            .withDestination(CustomDestination("topic", createTopic("topic")))
        )
        //#create-custom-jms-topic-sink
        val jmsTopicSink2: Sink[String, Future[Done]] = JmsProducer.textSink(
          JmsProducerSettings(producerConfig, connectionFactory)
            .withDestination(CustomDestination("topic", createTopic("topic")))
        )

        val in = List("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k")
        val inNumbers = (1 to 10).map(_.toString)

        //#create-custom-jms-topic-source
        val jmsTopicSource: Source[String, JmsConsumerControl] = JmsConsumer.textSource(
          JmsConsumerSettings(consumerConfig, connectionFactory)
            .withDestination(CustomDestination("topic", createTopic("topic")))
        )
        //#create-custom-jms-topic-source
        val jmsSource2: Source[String, JmsConsumerControl] = JmsConsumer.textSource(
          JmsConsumerSettings(consumerConfig, connectionFactory)
            .withDestination(CustomDestination("topic", createTopic("topic")))
        )

        val expectedSize = in.size + inNumbers.size

        import scala.concurrent.ExecutionContext.Implicits.global

        val result1 = jmsTopicSource.take(expectedSize).runWith(Sink.seq).map(_.sorted)
        val result2 = jmsSource2.take(expectedSize).runWith(Sink.seq).map(_.sorted)

        //We wait a little to be sure that the source is connected
        Thread.sleep(500)

        Source(in).runWith(jmsTopicSink)

        Source(inNumbers).runWith(jmsTopicSink2)

        val expectedList: List[String] = in ++ inNumbers
        result1.futureValue shouldEqual expectedList.sorted
        result2.futureValue shouldEqual expectedList.sorted
    }

    "publish and consume elements through a topic " in withConnectionFactory() { connectionFactory =>
      import system.dispatcher

      //#create-topic-sink
      val jmsTopicSink: Sink[String, Future[Done]] = JmsProducer.textSink(
        JmsProducerSettings(producerConfig, connectionFactory).withTopic("topic")
      )
      //#create-topic-sink
      val jmsTopicSink2: Sink[String, Future[Done]] = JmsProducer.textSink(
        JmsProducerSettings(producerConfig, connectionFactory).withTopic("topic")
      )

      val in = List("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k")
      val inNumbers = (1 to 10).map(_.toString)

      //#create-topic-source
      val jmsTopicSource: Source[String, JmsConsumerControl] = JmsConsumer.textSource(
        JmsConsumerSettings(consumerConfig, connectionFactory).withTopic("topic")
      )
      //#create-topic-source
      val jmsSource2: Source[String, JmsConsumerControl] = JmsConsumer.textSource(
        JmsConsumerSettings(consumerConfig, connectionFactory).withTopic("topic")
      )

      val expectedSize = in.size + inNumbers.size
      //#run-topic-source
      val result1 = jmsTopicSource.take(expectedSize).runWith(Sink.seq).map(_.sorted)
      val result2 = jmsSource2.take(expectedSize).runWith(Sink.seq).map(_.sorted)
      //#run-topic-source

      //We wait a little to be sure that the source is connected
      Thread.sleep(500)

      //#run-topic-sink
      Source(in).runWith(jmsTopicSink)
      //#run-topic-sink
      Source(inNumbers).runWith(jmsTopicSink2)

      val expectedList: List[String] = in ++ inNumbers
      result1.futureValue shouldEqual expectedList.sorted
      result2.futureValue shouldEqual expectedList.sorted
    }

    "publish and consume JMS text messages through a queue without acknowledgingg them" in withConnectionFactory() {
      connectionFactory =>
        val jmsSink: Sink[JmsTextMessage, Future[Done]] = JmsProducer.sink(
          JmsProducerSettings(producerConfig, connectionFactory).withQueue("numbers")
        )

        val msgsIn = (1 to 10).toList.map { n =>
          JmsTextMessage(n.toString)
        }

        Source(msgsIn).runWith(jmsSink)

        val jmsSource: Source[Message, JmsConsumerControl] = JmsConsumer(
          JmsConsumerSettings(consumerConfig, connectionFactory)
            .withQueue("numbers")
            .withAcknowledgeMode(AcknowledgeMode.ClientAcknowledge)
        )

        val result = jmsSource
          .take(msgsIn.size)
          .map {
            case textMessage: TextMessage =>
              textMessage.getText
            case other => fail(s"didn't match `$other`")
          }
          .runWith(Sink.seq)

        result.futureValue shouldEqual msgsIn.map(_.body)

        // messages were not acknowledged, may be delivered again
        jmsSource
          .takeWithin(5.seconds)
          .runWith(Sink.seq)
          .futureValue should not be empty
    }

    "sink successful completion" in withConnectionFactory() { connFactory =>
      val connectionFactory = new CachedConnectionFactory(connFactory)

      val jmsSink: Sink[JmsTextMessage, Future[Done]] = JmsProducer.sink(
        JmsProducerSettings(producerConfig, connectionFactory).withQueue("numbers")
      )

      val msgsIn = (1 to 10).toList.map { n =>
        JmsTextMessage(n.toString)
      }

      val completionFuture: Future[Done] = Source(msgsIn).runWith(jmsSink)
      completionFuture.futureValue shouldBe Done
      // make sure connection was closed
      eventually { connectionFactory.cachedConnection shouldBe Symbol("closed") }
    }

    "sink exceptional completion" in withConnectionFactory() { connFactory =>
      val connectionFactory = new CachedConnectionFactory(connFactory)

      val jmsSink: Sink[JmsTextMessage, Future[Done]] = JmsProducer.sink(
        JmsProducerSettings(producerConfig, connectionFactory)
          .withQueue("numbers")
          .withConnectionRetrySettings(ConnectionRetrySettings(system).withMaxRetries(0))
      )

      val completionFuture: Future[Done] = Source
        .failed[JmsTextMessage](new RuntimeException("Simulated error"))
        .runWith(jmsSink)

      completionFuture.failed.futureValue shouldBe a[RuntimeException]
      // make sure connection was closed
      eventually { connectionFactory.cachedConnection shouldBe Symbol("closed") }
    }

    "producer disconnect exceptional completion" in withServer() { server =>
      import system.dispatcher

      val connectionFactory = new CachedConnectionFactory(server.createConnectionFactory)

      val jmsSink: Sink[JmsTextMessage, Future[Done]] = JmsProducer.sink(
        JmsProducerSettings(producerConfig, connectionFactory)
          .withQueue("numbers")
          .withConnectionRetrySettings(ConnectionRetrySettings(system).withMaxRetries(2))
      )

      val completionFuture: Future[Done] = Source(0 to 10)
        .mapAsync(1)(
          n =>
            Future {
              Thread.sleep(100)
              JmsTextMessage(n.toString)
            }
        )
        .runWith(jmsSink)

      server.stop()

      val exception = completionFuture.failed.futureValue
      exception shouldBe a[ConnectionRetryException]
      exception.getCause shouldBe a[JMSException]

      // connection should be either
      // - not yet initialized before broker stop, or
      // - closed on broker stop (if preStart came first).
      if (connectionFactory.cachedConnection != null) {
        connectionFactory.cachedConnection shouldBe Symbol("closed")
      }
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

      val jmsSource: Source[Message, JmsConsumerControl] = JmsConsumer(
        JmsConsumerSettings(consumerConfig, connectionFactory)
          .withSessionCount(5)
          .withBufferSize(5)
          .withQueue("numbers")
      )

      val resultQueue = new LinkedBlockingQueue[String]()

      val (killSwitch, streamDone) = jmsSource
        .throttle(10, 1.second, 2, ThrottleMode.shaping)
        .toMat(Sink.foreach(msg => resultQueue.add(msg.asInstanceOf[TextMessage].getText)))(Keep.both)
        .run()

      // Need to wait for the stream to have started and running for sometime.
      Thread.sleep(2000)

      killSwitch.shutdown()

      streamDone.futureValue shouldBe Done

      // Keep publishing for another 2 seconds to make sure we killed the consumption mid-stream.
      Thread.sleep(2000)

      publishKillSwitch.shutdown()
      val numsIn = publishedData.futureValue

      // Ensure we break the stream while reading, not all input should have been read.
      resultQueue.size should be < numsIn.size

      val killSwitch2 = jmsSource
        .to(Sink.foreach(msg => resultQueue.add(msg.asInstanceOf[TextMessage].getText)))
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

      resultList.sortBy(_.toInt) should contain theSameElementsAs numsIn.map(_.toString)
    }

    "lose some elements when aborting a stream" in withConnectionFactory() { connectionFactory =>
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

      val jmsSource: Source[AckEnvelope, JmsConsumerControl] = JmsConsumer.ackSource(
        JmsConsumerSettings(consumerConfig, connectionFactory)
          .withSessionCount(5)
          .withBufferSize(5)
          .withQueue("numbers")
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

      // We may have lost some messages here, but most of them should have arrived.
      resultList.size should be > (numsIn.size / 2)
      resultList.size should be < numsIn.size
      resultList.size shouldBe resultList.toSet.size // no duplicates
    }

    "only fail after maxBackoff retry" in withServer() { server =>
      val connectionFactory = server.createConnectionFactory
      server.stop()
      val startTime = System.currentTimeMillis
      val result = JmsConsumer(
        JmsConsumerSettings(consumerConfig, connectionFactory)
          .withConnectionRetrySettings(ConnectionRetrySettings(system).withMaxRetries(4))
          .withQueue("test")
      ).runWith(Sink.seq)

      val ex = result.failed.futureValue
      val endTime = System.currentTimeMillis

      (endTime - startTime) shouldBe >(100L + 400L + 900L + 1600L)
      ex shouldBe a[ConnectionRetryException]
      ex.getCause shouldBe a[JMSException]
    }

    "browse" in withConnectionFactory() { connectionFactory =>
      val in = List(1 to 100).map(_.toString())

      withClue("write some messages") {
        Source(in)
          .runWith(JmsProducer.textSink(JmsProducerSettings(producerConfig, connectionFactory).withQueue("test")))
          .futureValue
      }

      withClue("browse the messages") {
        //#browse-source

        val browseSource: Source[jakarta.jms.Message, NotUsed] = JmsConsumer.browse(
          JmsBrowseSettings(system, connectionFactory)
            .withQueue("test")
        )

        val result: Future[immutable.Seq[jakarta.jms.Message]] =
          browseSource.runWith(Sink.seq)
        //#browse-source

        result.futureValue.collect { case msg: TextMessage => msg.getText } shouldEqual in
      }

      withClue("browse the messages again") {
        // the messages should not have been consumed
        val result = JmsConsumer
          .browse(JmsBrowseSettings(browseConfig, connectionFactory).withQueue("test"))
          .collect { case msg: TextMessage => msg.getText }
          .runWith(Sink.seq)

        result.futureValue shouldEqual in
      }
    }

    "producer flow" in withConnectionFactory() { connectionFactory =>
      //#flow-producer

      val flow: Flow[JmsMessage, JmsMessage, JmsProducerStatus] =
        JmsProducer.flow(
          JmsProducerSettings(system, connectionFactory)
            .withQueue("test")
        )

      val input: immutable.Seq[JmsTextMessage] =
        (1 to 100).map(i => JmsTextMessage(i.toString))

      val result: Future[Seq[JmsMessage]] = Source(input)
        .via(flow)
        .runWith(Sink.seq)
      //#flow-producer

      result.futureValue should ===(input)
    }

    "accept message-defined destinations" in withConnectionFactory() { connectionFactory =>
      //#run-directed-flow-producer
      val flowSink: Flow[JmsMessage, JmsMessage, JmsProducerStatus] =
        JmsProducer.flow(
          JmsProducerSettings(system, connectionFactory).withQueue("test")
        )

      val input = (1 to 100).map { i =>
        val queueName = if (i % 2 == 0) "even" else "odd"
        JmsTextMessage(i.toString).toQueue(queueName)
      }
      Source(input).via(flowSink).runWith(Sink.ignore)
      //#run-directed-flow-producer

      val jmsEvenSource: Source[String, JmsConsumerControl] = JmsConsumer.textSource(
        JmsConsumerSettings(consumerConfig, connectionFactory).withQueue("even")
      )
      val jmsOddSource: Source[String, JmsConsumerControl] = JmsConsumer.textSource(
        JmsConsumerSettings(consumerConfig, connectionFactory).withQueue("odd")
      )

      jmsEvenSource.take(input.size / 2).map(_.toInt).runWith(Sink.seq).futureValue shouldBe (2 to 100 by 2)
      jmsOddSource.take(input.size / 2).map(_.toInt).runWith(Sink.seq).futureValue shouldBe (1 to 99 by 2)
    }

    "fail if message destination is not defined" in {
      val connectionFactory = new ActiveMQConnectionFactory("localhost:1234")

      an[IllegalArgumentException] shouldBe thrownBy {
        JmsProducer.flow(JmsProducerSettings(producerConfig, connectionFactory))
      }
    }

    "publish and consume strings through a queue with multiple sessions" in withConnectionFactory() {
      connectionFactory =>
        val jmsSink: Sink[String, Future[Done]] = JmsProducer.textSink(
          JmsProducerSettings(producerConfig, connectionFactory).withQueue("test").withSessionCount(5)
        )

        val in = List("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k")
        val sinkOut = Source(in).runWith(jmsSink)

        val jmsSource: Source[String, JmsConsumerControl] = JmsConsumer.textSource(
          JmsConsumerSettings(consumerConfig, connectionFactory).withSessionCount(5).withQueue("test")
        )

        val result = jmsSource.take(in.size).runWith(Sink.seq)

        sinkOut.futureValue shouldBe Done
        result.futureValue should contain allElementsOf in
    }

    "produce elements in order" in withMockedProducer { ctx =>
      import ctx._
      val delays = new AtomicInteger()
      val textMessage = mock(classOf[TextMessage])

      val delayedSend = new Answer[Unit] {
        override def answer(invocation: InvocationOnMock): Unit = {
          delays.incrementAndGet()
          Thread.sleep(ThreadLocalRandom.current().nextInt(1, 10))
        }
      }

      when(session.createTextMessage(anyString())).thenReturn(textMessage)
      when(producer.send(any[jakarta.jms.Destination], any[Message], anyInt(), anyInt(), anyLong()))
        .thenAnswer(delayedSend)

      val in = (1 to 50).map(i => JmsTextMessage(i.toString))
      val jmsFlow = JmsProducer.flow[JmsTextMessage](
        JmsProducerSettings(producerConfig, factory).withQueue("test").withSessionCount(8)
      )

      val result = Source(in).via(jmsFlow).toMat(Sink.seq)(Keep.right).run()

      result.futureValue shouldEqual in
      delays.get shouldBe 50
    }

    "fail fast on the first failing send" in withMockedProducer { ctx =>
      import ctx._
      val sendLatch = new CountDownLatch(3)
      val receiveLatch = new CountDownLatch(3)

      val messages = (1 to 10).map(i => mock(classOf[TextMessage]) -> i).toMap
      messages.foreach {
        case (msg, i) => when(session.createTextMessage(i.toString)).thenReturn(msg)
      }

      val failOnFifthAndDelayFourthItem = new Answer[Unit] {
        override def answer(invocation: InvocationOnMock): Unit = {
          val msgNo = messages(invocation.getArgument[TextMessage](1))
          msgNo match {
            case 1 | 2 | 3 =>
              sendLatch.countDown() // first three sends work...
            case 4 =>
              Thread.sleep(30000) // this one gets delayed...
            case 5 =>
              sendLatch.await()
              receiveLatch.await()
              throw new RuntimeException("Mocked send failure") // this one fails.
            case _ => ()
          }
        }
      }
      when(producer.send(any[jakarta.jms.Destination], any[Message], anyInt(), anyInt(), anyLong()))
        .thenAnswer(failOnFifthAndDelayFourthItem)

      val in = (1 to 10).map(i => JmsTextMessage(i.toString))
      val done = JmsTextMessage("done")
      val jmsFlow = JmsProducer.flow[JmsTextMessage](
        JmsProducerSettings(producerConfig, factory).withQueue("test").withSessionCount(8)
      )
      val result = Source(in)
        .via(jmsFlow)
        .alsoTo(Sink.foreach(_ => receiveLatch.countDown()))
        .recover { case _ => done }
        .toMat(Sink.seq)(Keep.right)
        .run()

      // expect send failure on no 5. to cause immediate stream failure (after no. 1, 2 and 3),
      // even though no 4. is still in-flight.
      result.futureValue shouldEqual in.take(3) :+ done
    }

    "put back JmsProducer to the pool when send fails" in withMockedProducer { ctx =>
      import ctx._

      val messages = (1 to 10).map(i => mock(classOf[TextMessage]) -> i).toMap
      messages.foreach {
        case (msg, i) => when(session.createTextMessage(i.toString)).thenReturn(msg)
      }

      val failOnEvenCount = new Answer[Unit] {
        override def answer(invocation: InvocationOnMock): Unit = {
          val msgNo = messages(invocation.getArgument[TextMessage](1))
          if (msgNo % 2 == 0) throw new RuntimeException("Mocked send failure")
        }
      }

      when(producer.send(any[jakarta.jms.Destination], any[Message], anyInt(), anyInt(), anyLong()))
        .thenAnswer(failOnEvenCount)

      val decider: Supervision.Decider = {
        case _: RuntimeException => Supervision.Resume
        case _ => Supervision.Stop
      }
      val jmsFlow = JmsProducer
        .flow[JmsTextMessage](JmsProducerSettings(producerConfig, factory).withQueue("test").withSessionCount(2))
        .withAttributes(ActorAttributes.supervisionStrategy(decider))

      val in = (1 to 10).map(i => JmsTextMessage(i.toString))
      val result = Source(in).via(jmsFlow).toMat(Sink.seq)(Keep.right).run()

      result.futureValue.map(_.body.toInt) shouldEqual Seq(1, 3, 5, 7, 9)
    }

    val mockMessages = (0 until 10).map(_.toString)

    val mockMessageGenerator = new Answer[Unit]() {
      override def answer(invocation: InvocationOnMock): Unit = {
        val listener = invocation.getArgument[MessageListener](0)
        mockMessages.foreach { s =>
          val message = mock(classOf[TextMessage])
          when(message.getText).thenReturn(s)
          listener.onMessage(message)
        }
      }
    }

    "reconnect when timing out establishing a connection" in {
      val factory = mock(classOf[ConnectionFactory])
      val connection = mock(classOf[Connection])
      val session = mock(classOf[Session])
      val consumer = mock(classOf[MessageConsumer])
      @volatile var connectCount = 0
      val connectTimeout = 2.seconds
      val connectDelay = 10.seconds

      when(factory.createConnection()).thenAnswer(new Answer[Connection]() {
        override def answer(invocation: InvocationOnMock): Connection = {
          connectCount += 1
          if (connectCount == 1) Thread.sleep(connectDelay.toMillis) // Cause a connect timeout
          connection
        }
      })

      when(connection.createSession(anyBoolean(), anyInt())).thenReturn(session)
      when(session.createConsumer(any[jakarta.jms.Destination])).thenReturn(consumer)
      when(consumer.setMessageListener(any[MessageListener])).thenAnswer(mockMessageGenerator)

      val jmsSource = JmsConsumer.textSource(
        JmsConsumerSettings(consumerConfig, factory)
          .withQueue("test")
          .withConnectionRetrySettings(ConnectionRetrySettings(system).withConnectTimeout(connectTimeout))
      )

      val startTime = System.nanoTime
      val resultFuture = jmsSource.take(10).runWith(Sink.seq)
      val result = resultFuture.futureValue
      val timeTaken = Duration.fromNanos(System.nanoTime - startTime)

      result should contain theSameElementsAs mockMessages
      connectCount shouldBe 2
      timeTaken should be > connectTimeout
      timeTaken should be < connectDelay
    }

    "reconnect when timing out starting a connection" in {
      val factory = mock(classOf[ConnectionFactory])
      val connection = mock(classOf[Connection])
      val session = mock(classOf[Session])
      val consumer = mock(classOf[MessageConsumer])
      @volatile var connectStartCount = 0
      val connectTimeout = 2.seconds
      val connectStartDelay = 10.seconds

      when(factory.createConnection()).thenReturn(connection)

      when(connection.start()).thenAnswer(new Answer[Unit]() {
        override def answer(invocation: InvocationOnMock): Unit = {
          connectStartCount += 1
          if (connectStartCount == 1) Thread.sleep(connectStartDelay.toMillis) // first connection start to timeout
        }
      })

      when(connection.createSession(anyBoolean(), anyInt())).thenReturn(session)
      when(session.createConsumer(any[jakarta.jms.Destination])).thenReturn(consumer)
      when(consumer.setMessageListener(any[MessageListener])).thenAnswer(mockMessageGenerator)

      val jmsSource = JmsConsumer.textSource(
        JmsConsumerSettings(consumerConfig, factory)
          .withQueue("test")
          .withConnectionRetrySettings(ConnectionRetrySettings(system).withConnectTimeout(connectTimeout))
      )

      val startTime = System.nanoTime
      val resultFuture = jmsSource.take(10).runWith(Sink.seq)
      val result = resultFuture.futureValue
      val timeTaken = Duration.fromNanos(System.nanoTime - startTime)

      result should contain theSameElementsAs mockMessages
      connectStartCount shouldBe 2
      timeTaken should be > connectTimeout
      timeTaken should be < connectStartDelay
    }

    "reconnect when runtime connection exception occurs" in {
      val factory = mock(classOf[ConnectionFactory])
      val connection = mock(classOf[Connection])
      val session = mock(classOf[Session])
      val consumer = mock(classOf[MessageConsumer])
      @volatile var connectCount = 0
      @volatile var exceptionListener: Option[ExceptionListener] = None
      val messageGroups = mockMessages.grouped(3)

      when(factory.createConnection()).thenAnswer(new Answer[Connection]() {
        override def answer(invocation: InvocationOnMock): Connection = {
          connectCount += 1
          connection
        }
      })

      when(connection.createSession(anyBoolean(), anyInt())).thenReturn(session)

      when(connection.setExceptionListener(any[ExceptionListener])).thenAnswer(new Answer[Unit]() {
        override def answer(invocation: InvocationOnMock): Unit =
          exceptionListener = Option(invocation.getArgument[ExceptionListener](0))
      })

      when(session.createConsumer(any[jakarta.jms.Destination])).thenReturn(consumer)

      when(consumer.setMessageListener(any[MessageListener])).thenAnswer(new Answer[Unit]() {
        override def answer(invocation: InvocationOnMock): Unit = {
          val listener = invocation.getArgument[MessageListener](0)
          val thisMessageGroup = messageGroups.next()
          thisMessageGroup.foreach { s =>
            val message = mock(classOf[TextMessage])
            when(message.getText).thenReturn(s)
            listener.onMessage(message)
          }
          if (messageGroups.hasNext) {
            exceptionListener.foreach(_.onException(new JMSException("Mock: causing an exception while consuming")))
          }
        }
      })

      val jmsSource = JmsConsumer.textSource(
        JmsConsumerSettings(consumerConfig, factory)
          .withQueue("test")
      )

      val resultFuture = jmsSource.take(mockMessages.size).runWith(Sink.seq)

      resultFuture.futureValue should contain theSameElementsAs mockMessages

      // Connects 1 time at start and 3 times each 3 messages.
      connectCount shouldBe 4
    }

    "pass through message envelopes" in withConnectionFactory() { connectionFactory =>
      //#run-flexi-flow-producer
      val jmsProducer: Flow[JmsEnvelope[String], JmsEnvelope[String], JmsProducerStatus] =
        JmsProducer.flexiFlow[String](
          JmsProducerSettings(system, connectionFactory).withQueue("test")
        )

      val data = List("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k")
      val in: immutable.Seq[JmsTextMessagePassThrough[String]] =
        data.map(t => JmsTextMessage(t).withPassThrough(t))

      val result = Source(in)
        .via(jmsProducer)
        .map(_.passThrough) // extract the value passed through
        .runWith(Sink.seq)
      //#run-flexi-flow-producer

      result.futureValue shouldEqual data

      val sentData =
        JmsConsumer
          .textSource(JmsConsumerSettings(consumerConfig, connectionFactory).withQueue("test"))
          .take(data.size)
          .runWith(Sink.seq)

      sentData.futureValue shouldEqual data
    }

    "pass through empty envelopes" in {
      val connectionFactory = mock(classOf[ConnectionFactory])
      val connection = mock(classOf[Connection])
      val session = mock(classOf[Session])
      val producer = mock(classOf[MessageProducer])
      val message = mock(classOf[TextMessage])

      when(connectionFactory.createConnection()).thenReturn(connection)
      when(connection.createSession(anyBoolean(), anyInt())).thenReturn(session)
      when(session.createProducer(any[jakarta.jms.Destination])).thenReturn(producer)
      when(session.createTextMessage(any[String])).thenReturn(message)

      //#run-flexi-flow-pass-through-producer
      val jmsProducer = JmsProducer.flexiFlow[String](
        JmsProducerSettings(producerConfig, connectionFactory).withQueue("topic")
      )

      val data = List("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k")
      val in = data.map(t => JmsPassThrough(t))

      val result = Source(in).via(jmsProducer).map(_.passThrough).runWith(Sink.seq)
      //#run-flexi-flow-pass-through-producer

      result.futureValue shouldEqual data

      verify(session, never()).createTextMessage(any[String])
      verify(producer, never()).send(any[jakarta.jms.Destination], any[Message], any[Int], any[Int], any[Long])
    }
  }

  "publish and subscribe with a durable subscription" in withServer() { server =>
    import org.apache.activemq.ActiveMQConnectionFactory
    val producerConnectionFactory = server.createConnectionFactory
    //#create-connection-factory-with-client-id
    val consumerConnectionFactory = server.createConnectionFactory.asInstanceOf[ActiveMQConnectionFactory]
    consumerConnectionFactory.setClientID(getClass.getSimpleName)
    //#create-connection-factory-with-client-id

    val jmsTopicSink: Sink[String, Future[Done]] = JmsProducer.textSink(
      JmsProducerSettings(producerConfig, producerConnectionFactory).withTopic("topic")
    )

    val in = List("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k")

    //#create-durable-topic-source
    val jmsTopicSource = JmsConsumer.textSource(
      JmsConsumerSettings(consumerConfig, consumerConnectionFactory)
        .withDurableTopic("topic", "durable-test")
    )
    //#create-durable-topic-source

    //#run-durable-topic-source
    val result = jmsTopicSource.take(in.size).runWith(Sink.seq)
    //#run-durable-topic-source

    // We wait a little to be sure that the source is connected
    Thread.sleep(500)

    Source(in).runWith(jmsTopicSink)

    result.futureValue shouldEqual in
  }

  "support request/reply with temporary queues" in withConnectionFactory() { connectionFactory =>
    val connection = connectionFactory.createConnection()
    connection.start()
    val session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val tempQueue = session.createTemporaryQueue()
    val tempQueueDest = alpakka.jakartajms.Destination(tempQueue)
    val message = "ThisIsATest"
    val correlationId = UUID.randomUUID().toString

    val toRespondSink: Sink[JmsMessage, Future[Done]] = JmsProducer.sink(
      JmsProducerSettings(system, connectionFactory).withQueue("test")
    )

    val toRespondStreamCompletion: Future[Done] =
      Source
        .single(
          JmsTextMessage(message)
            .withHeader(JmsCorrelationId(correlationId))
            .withHeader(JmsReplyTo(tempQueueDest))
        )
        .runWith(toRespondSink)

    //#request-reply
    val respondStreamControl: JmsConsumerControl =
      JmsConsumer(JmsConsumerSettings(system, connectionFactory).withQueue("test"))
        .collect {
          case message: TextMessage => JmsTextMessage(message)
        }
        .map { textMessage =>
          textMessage.headers.foldLeft(JmsTextMessage(textMessage.body.reverse)) {
            case (acc, rt: JmsReplyTo) => acc.to(rt.jmsDestination)
            case (acc, cId: JmsCorrelationId) => acc.withHeader(cId)
            case (acc, _) => acc
          }
        }
        .via {
          JmsProducer.flow(
            JmsProducerSettings(system, connectionFactory).withQueue("ignored")
          )
        }
        .to(Sink.ignore)
        .run()
    //#request-reply

    // getting ConnectionRetryException when trying to listen using streams, assuming it's because
    // a different session can't listen on the original session's TemporaryQueue
    val consumer = session.createConsumer(tempQueue)
    val result = Future(consumer.receive(5.seconds.toMillis))(system.dispatcher)

    toRespondStreamCompletion.futureValue shouldEqual Done

    val msg = result.futureValue
    msg shouldBe a[TextMessage]
    msg.getJMSCorrelationID shouldEqual correlationId
    msg.asInstanceOf[TextMessage].getText shouldEqual message.reverse

    respondStreamControl.shutdown()

    connection.close()
  }
}
