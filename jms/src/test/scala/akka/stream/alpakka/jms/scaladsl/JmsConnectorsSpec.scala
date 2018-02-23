/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms.scaladsl

import java.nio.charset.Charset
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import javax.jms.{DeliveryMode, JMSException, Message, TextMessage}

import akka.stream.alpakka.jms._
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.stream.{KillSwitch, KillSwitches, ThrottleMode}
import akka.{Done, NotUsed}
import org.apache.activemq.ActiveMQConnectionFactory
import org.apache.activemq.command.ActiveMQQueue

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.immutable.Seq
import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

final case class DummyObject(payload: String)

class JmsConnectorsSpec extends JmsSpec {

  override implicit val patienceConfig = PatienceConfig(2.minutes)

  "The JMS Connectors" should {
    "publish and consume strings through a queue" in withServer() { ctx =>
      //#connection-factory
      val connectionFactory: javax.jms.ConnectionFactory = new ActiveMQConnectionFactory(ctx.url)
      //#connection-factory

      //#create-text-sink
      val jmsSink: Sink[String, Future[Done]] = JmsProducer.textSink(
        JmsProducerSettings(connectionFactory).withQueue("test")
      )
      //#create-text-sink

      //#run-text-sink
      val in = List("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k")
      Source(in).runWith(jmsSink)
      //#run-text-sink

      //#create-text-source
      val jmsSource: Source[String, KillSwitch] = JmsConsumer.textSource(
        JmsConsumerSettings(connectionFactory).withBufferSize(10).withQueue("test")
      )
      //#create-text-source

      //#run-text-source
      val result = jmsSource.take(in.size).runWith(Sink.seq)
      //#run-text-source

      result.futureValue shouldEqual in
    }

    "publish and consume serializable objects through a queue" in withServer() { ctx =>
      //#connection-factory-object
      val connectionFactory = new ActiveMQConnectionFactory(ctx.url)
      connectionFactory.setTrustedPackages(List(classOf[DummyObject].getPackage.getName).asJava)
      //#connection-factory-object

      //#create-object-sink
      val jmsSink: Sink[Serializable, Future[Done]] = JmsProducer.objectSink(
        JmsProducerSettings(connectionFactory).withQueue("test")
      )
      //#create-object-sink

      //#run-object-sink

      val in = DummyObject("ThisIsATest")
      Source.single(in).runWith(jmsSink)
      //#run-object-sink

      //#create-object-source
      val jmsSource: Source[java.io.Serializable, KillSwitch] = JmsConsumer.objectSource(
        JmsConsumerSettings(connectionFactory).withQueue("test")
      )
      //#create-object-source

      //#run-object-source
      val result = jmsSource.take(1).runWith(Sink.head)
      //#run-object-source

      result.futureValue shouldEqual in
    }

    "publish and consume bytearray through a queue" in withServer() { ctx =>
      val connectionFactory = new ActiveMQConnectionFactory(ctx.url)

      //#create-bytearray-sink
      val jmsSink: Sink[Array[Byte], Future[Done]] = JmsProducer.bytesSink(
        JmsProducerSettings(connectionFactory).withQueue("test")
      )
      //#create-bytearray-sink

      //#run-bytearray-sink
      val in = "ThisIsATest".getBytes(Charset.forName("UTF-8"))
      Source.single(in).runWith(jmsSink)
      //#run-bytearray-sink

      //#create-bytearray-source
      val jmsSource: Source[Array[Byte], KillSwitch] = JmsConsumer.bytesSource(
        JmsConsumerSettings(connectionFactory).withQueue("test")
      )
      //#create-bytearray-source

      //#run-bytearray-source
      val result = jmsSource.take(1).runWith(Sink.head)
      //#run-bytearray-source

      result.futureValue shouldEqual in
    }

    "publish and consume map through a queue" in withServer() { ctx =>
      val connectionFactory = new ActiveMQConnectionFactory(ctx.url)

      //#create-map-sink
      val jmsSink: Sink[Map[String, Any], Future[Done]] = JmsProducer.mapSink(
        JmsProducerSettings(connectionFactory).withQueue("test")
      )
      //#create-map-sink

      //#run-map-sink
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

      Source(input).runWith(jmsSink)
      //#run-map-sink

      //#create-map-source
      val jmsSource: Source[Map[String, Any], KillSwitch] = JmsConsumer.mapSource(
        JmsConsumerSettings(connectionFactory).withQueue("test")
      )
      //#create-map-source

      //#run-map-source
      val result = jmsSource.take(1).runWith(Sink.seq)
      //#run-map-source

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

    "publish and consume JMS text messages with properties through a queue" in withServer() { ctx =>
      val connectionFactory = new ActiveMQConnectionFactory(ctx.url)

      val jmsSink: Sink[JmsTextMessage, Future[Done]] = JmsProducer(
        JmsProducerSettings(connectionFactory).withQueue("numbers")
      )

      //#create-messages-with-properties
      val msgsIn = (1 to 10).toList.map { n =>
        JmsTextMessage(n.toString)
          .withProperty("Number", n)
          .withProperty("IsOdd", n % 2 == 1)
          .withProperty("IsEven", n % 2 == 0)
      }
      //#create-messages-with-properties

      Source(msgsIn).runWith(jmsSink)

      //#create-jms-source
      val jmsSource: Source[Message, KillSwitch] = JmsConsumer(
        JmsConsumerSettings(connectionFactory).withBufferSize(10).withQueue("numbers")
      )
      //#create-jms-source

      //#run-jms-source
      val result: Future[Seq[Message]] = jmsSource.take(msgsIn.size).runWith(Sink.seq)
      //#run-jms-source

      // The sent message and the receiving one should have the same properties
      result.futureValue.zip(msgsIn).foreach {
        case (out, in) =>
          out.getIntProperty("Number") shouldEqual in.properties("Number")
          out.getBooleanProperty("IsOdd") shouldEqual in.properties("IsOdd")
          out.getBooleanProperty("IsEven") shouldEqual in.properties("IsEven")
      }
    }

    "publish and consume JMS text messages with header through a queue" in withServer() { ctx =>
      val connectionFactory = new ActiveMQConnectionFactory(ctx.url)

      //#create-jms-sink
      val jmsSink: Sink[JmsTextMessage, Future[Done]] = JmsProducer(
        JmsProducerSettings(connectionFactory).withQueue("numbers")
      )
      //#create-jms-sink

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

      val jmsSource: Source[Message, KillSwitch] = JmsConsumer(
        JmsConsumerSettings(connectionFactory).withBufferSize(10).withQueue("numbers")
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

    "publish JMS text messages with properties through a queue and consume them with a selector" in withServer() {
      ctx =>
        val connectionFactory = new ActiveMQConnectionFactory(ctx.url)

        val jmsSink: Sink[JmsTextMessage, Future[Done]] = JmsProducer(
          JmsProducerSettings(connectionFactory).withQueue("numbers")
        )

        val msgsIn = (1 to 10).toList.map { n =>
          JmsTextMessage(n.toString)
            .withProperty("Number", n)
            .withProperty("IsOdd", n % 2 == 1)
            .withProperty("IsEven", n % 2 == 0)
        }
        Source(msgsIn).runWith(jmsSink)

        //#create-jms-source-with-selector
        val jmsSource = JmsConsumer(
          JmsConsumerSettings(connectionFactory).withBufferSize(10).withQueue("numbers").withSelector("IsOdd = TRUE")
        )
        //#create-jms-source-with-selector

        //#assert-only-odd-messages-received
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
      //#assert-only-odd-messages-received
    }

    "applying backpressure when the consumer is slower than the producer" in withServer() { ctx =>
      val connectionFactory = new ActiveMQConnectionFactory(ctx.url)
      val in = List("a", "b", "c")
      Source(in).runWith(JmsProducer.textSink(JmsProducerSettings(connectionFactory).withQueue("test")))

      val result = JmsConsumer
        .textSource(JmsConsumerSettings(connectionFactory).withBufferSize(1).withQueue("test"))
        .throttle(1, 1.second, 1, ThrottleMode.shaping)
        .take(in.size)
        .runWith(Sink.seq)

      result.futureValue shouldEqual in
    }

    "disconnection should fail the stage" in withServer() { ctx =>
      val connectionFactory = new ActiveMQConnectionFactory(ctx.url)
      val result = JmsConsumer(JmsConsumerSettings(connectionFactory).withQueue("test")).runWith(Sink.seq)
      Thread.sleep(500)
      ctx.broker.stop()
      result.failed.futureValue shouldBe an[JMSException]
    }

    "publish and consume elements through a topic " in withServer() { ctx =>
      import system.dispatcher

      val connectionFactory = new ActiveMQConnectionFactory(ctx.url)

      //#create-topic-sink
      val jmsTopicSink: Sink[String, Future[Done]] = JmsProducer.textSink(
        JmsProducerSettings(connectionFactory).withTopic("topic")
      )
      //#create-topic-sink
      val jmsTopicSink2: Sink[String, Future[Done]] = JmsProducer.textSink(
        JmsProducerSettings(connectionFactory).withTopic("topic")
      )

      val in = List("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k")
      val inNumbers = (1 to 10).map(_.toString)

      //#create-topic-source
      val jmsTopicSource: Source[String, KillSwitch] = JmsConsumer.textSource(
        JmsConsumerSettings(connectionFactory).withBufferSize(10).withTopic("topic")
      )
      //#create-topic-source
      val jmsSource2: Source[String, KillSwitch] = JmsConsumer.textSource(
        JmsConsumerSettings(connectionFactory).withBufferSize(10).withTopic("topic")
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

    "publish and consume JMS text messages through a queue with client ack" in withServer() { ctx =>
      val connectionFactory = new ActiveMQConnectionFactory(ctx.url)

      val jmsSink: Sink[JmsTextMessage, Future[Done]] = JmsProducer(
        JmsProducerSettings(connectionFactory).withQueue("numbers")
      )

      val msgsIn = (1 to 10).toList.map { n =>
        JmsTextMessage(n.toString)
      }

      Source(msgsIn).runWith(jmsSink)

      //#create-jms-source-client-ack
      val jmsSource: Source[Message, KillSwitch] = JmsConsumer(
        JmsConsumerSettings(connectionFactory)
          .withQueue("numbers")
          .withAcknowledgeMode(AcknowledgeMode.ClientAcknowledge)
      )
      //#create-jms-source-client-ack

      //#run-jms-source-with-ack
      val result = jmsSource
        .take(msgsIn.size)
        .map {
          case textMessage: TextMessage =>
            val text = textMessage.getText
            textMessage.acknowledge()
            text
        }
        .runWith(Sink.seq)
      //#run-jms-source-with-ack

      result.futureValue shouldEqual msgsIn.map(_.body)

      // all messages were acknowledged before
      jmsSource
        .takeWithin(5.seconds)
        .runWith(Sink.seq)
        .futureValue shouldBe empty
    }

    "publish and consume JMS text messages through a queue without acknowledgingg them" in withServer() { ctx =>
      val connectionFactory = new ActiveMQConnectionFactory(ctx.url)

      val jmsSink: Sink[JmsTextMessage, Future[Done]] = JmsProducer(
        JmsProducerSettings(connectionFactory).withQueue("numbers")
      )

      val msgsIn = (1 to 10).toList.map { n =>
        JmsTextMessage(n.toString)
      }

      Source(msgsIn).runWith(jmsSink)

      val jmsSource: Source[Message, KillSwitch] = JmsConsumer(
        JmsConsumerSettings(connectionFactory)
          .withBufferSize(10)
          .withQueue("numbers")
          .withAcknowledgeMode(AcknowledgeMode.ClientAcknowledge)
      )

      val result = jmsSource
        .take(msgsIn.size)
        .map {
          case textMessage: TextMessage =>
            textMessage.getText
        }
        .runWith(Sink.seq)

      result.futureValue shouldEqual msgsIn.map(_.body)

      // messages were not acknowledged, may be delivered again
      jmsSource
        .takeWithin(5.seconds)
        .runWith(Sink.seq)
        .futureValue should not be empty
    }

    "sink successful completion" in withServer() { ctx =>
      val url: String = ctx.url
      val connectionFactory = new CachedConnectionFactory(url)

      val jmsSink: Sink[JmsTextMessage, Future[Done]] = JmsProducer(
        JmsProducerSettings(connectionFactory).withQueue("numbers")
      )

      val msgsIn = (1 to 10).toList.map { n =>
        JmsTextMessage(n.toString)
      }

      val completionFuture: Future[Done] = Source(msgsIn).runWith(jmsSink)
      completionFuture.futureValue shouldBe Done
      // make sure connection was closed
      connectionFactory.cachedConnection.isClosed shouldBe true
    }

    "sink exceptional completion" in withServer() { ctx =>
      val url: String = ctx.url
      val connectionFactory = new CachedConnectionFactory(url)

      val jmsSink: Sink[JmsTextMessage, Future[Done]] = JmsProducer(
        JmsProducerSettings(connectionFactory).withQueue("numbers")
      )

      val completionFuture: Future[Done] = Source
        .failed[JmsTextMessage](new RuntimeException("Simulated error"))
        .runWith(jmsSink)
      completionFuture.failed.futureValue shouldBe an[RuntimeException]
      // make sure connection was closed
      connectionFactory.cachedConnection.isClosed shouldBe true
    }

    "sink disconnect exceptional completion" in withServer() { ctx =>
      import system.dispatcher

      val url: String = ctx.url
      val connectionFactory = new CachedConnectionFactory(url)

      val jmsSink: Sink[JmsTextMessage, Future[Done]] = JmsProducer(
        JmsProducerSettings(connectionFactory).withQueue("numbers")
      )

      val completionFuture: Future[Done] = Source(0 to 10)
        .mapAsync(1)(
          n =>
            Future {
              Thread.sleep(500)
              JmsTextMessage(n.toString)
          }
        )
        .runWith(jmsSink)

      ctx.broker.stop()

      completionFuture.failed.futureValue shouldBe a[JMSException]
      // connection was not yet initialized before broker stop
      connectionFactory.cachedConnection shouldBe null
    }

    "ensure no message loss when stopping a stream" in withServer() { ctx =>
      val connectionFactory = new ActiveMQConnectionFactory(ctx.url)

      val jmsSink: Sink[JmsTextMessage, Future[Done]] = JmsProducer(
        JmsProducerSettings(connectionFactory).withQueue("numbers")
      )

      val (publishKillSwitch, publishedData) = Source
        .unfold(1)(n => Some(n + 1 -> n))
        .throttle(15, 1.second, 2, ThrottleMode.shaping) // Higher than consumption rate.
        .viaMat(KillSwitches.single)(Keep.right)
        .alsoTo(Flow[Int].map(n => JmsTextMessage(n.toString).withProperty("Number", n)).to(jmsSink))
        .toMat(Sink.seq)(Keep.both)
        .run()

      val jmsSource: Source[Message, KillSwitch] = JmsConsumer(
        JmsConsumerSettings(connectionFactory).withSessionCount(5).withBufferSize(5).withQueue("numbers")
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

    "lose some elements when aborting a stream" in withServer() { ctx =>
      val connectionFactory = new ActiveMQConnectionFactory(ctx.url)

      val jmsSink: Sink[JmsTextMessage, Future[Done]] = JmsProducer(
        JmsProducerSettings(connectionFactory).withQueue("numbers")
      )

      val (publishKillSwitch, publishedData) = Source
        .unfold(1)(n => Some(n + 1 -> n))
        .throttle(15, 1.second, 2, ThrottleMode.shaping) // Higher than consumption rate.
        .viaMat(KillSwitches.single)(Keep.right)
        .alsoTo(Flow[Int].map(n => JmsTextMessage(n.toString).withProperty("Number", n)).to(jmsSink))
        .toMat(Sink.seq)(Keep.both)
        .run()

      val jmsSource: Source[AckEnvelope, KillSwitch] = JmsConsumer.ackSource(
        JmsConsumerSettings(connectionFactory).withSessionCount(5).withBufferSize(5).withQueue("numbers")
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

    "browse" in withServer() { ctx =>
      val connectionFactory = new ActiveMQConnectionFactory(ctx.url)
      val in = List(1 to 100).map(_.toString())

      withClue("write some messages") {
        Source(in)
          .runWith(JmsProducer.textSink(JmsProducerSettings(connectionFactory).withQueue("test")))
          .futureValue
      }

      withClue("browse the messages") {
        //#create-browse-source
        val browseSource: Source[Message, NotUsed] = JmsConsumer.browse(
          JmsBrowseSettings(connectionFactory).withQueue("test")
        )
        //#create-browse-source

        //#run-browse-source
        val result = browseSource.runWith(Sink.seq)
        //#run-browse-source

        result.futureValue.collect { case msg: TextMessage => msg.getText } shouldEqual in
      }

      withClue("browse the messages again") {
        // the messages should not have been consumed
        val result = JmsConsumer
          .browse(JmsBrowseSettings(connectionFactory).withQueue("test"))
          .collect { case msg: TextMessage => msg.getText }
          .runWith(Sink.seq)

        result.futureValue shouldEqual in
      }
    }

    "producer flow" in withServer() { ctx =>
      val connectionFactory = new ActiveMQConnectionFactory(ctx.url)

      //#create-flow-producer
      val flowSink: Flow[JmsMessage, JmsMessage, NotUsed] = JmsProducer.flow(
        JmsProducerSettings(connectionFactory).withQueue("test")
      )
      //#create-flow-producer

      //#run-flow-producer
      val input = (1 to 100).map(i => JmsTextMessage(i.toString))

      val result = Source(input)
        .via(flowSink)
        .runWith(Sink.seq)
      //#run-flow-producer

      result.futureValue should ===(input)
    }
  }
}
