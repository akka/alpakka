/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.jms.scaladsl

import javax.jms.{JMSException, Message, TextMessage}

import akka.NotUsed
import akka.stream.ThrottleMode
import akka.stream.alpakka.jms._
import akka.stream.scaladsl.{Flow, Sink, Source}
import org.apache.activemq.ActiveMQConnectionFactory

import scala.concurrent.duration._
import collection.JavaConverters._

class JmsConnectorsSpec extends JmsSpec {

  override implicit val patienceConfig = PatienceConfig(1.minute)

  "The JMS Connectors" should {
    "publish and consume strings through a queue" in withServer() { ctx =>
      val url: String = ctx.url
      //#connection-factory
      val connectionFactory = new ActiveMQConnectionFactory(url)
      //#connection-factory

      //#create-text-sink
      val jmsSink: Sink[String, NotUsed] = JmsSink.textSink(
        JmsSinkSettings(connectionFactory).withQueue("test")
      )
      //#create-text-sink

      //#run-text-sink
      val in = List("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k")
      Source(in).runWith(jmsSink)
      //#run-text-sink

      //#create-text-source
      val jmsSource: Source[String, NotUsed] = JmsSource.textSource(
        JmsSourceSettings(connectionFactory).withBufferSize(10).withQueue("test")
      )
      //#create-text-source

      //#run-text-source
      val result = jmsSource.take(in.size).runWith(Sink.seq)
      //#run-text-source

      result.futureValue shouldEqual in
    }

    "publish and consume JMS text messages with properties through a queue" in withServer() { ctx =>
      val url: String = ctx.url
      val connectionFactory = new ActiveMQConnectionFactory(url)

      //#create-jms-sink
      val jmsSink: Sink[JmsTextMessage, NotUsed] = JmsSink(
        JmsSinkSettings(connectionFactory).withQueue("numbers")
      )
      //#create-jms-sink

      //#create-messages-with-properties
      val msgsIn = (1 to 10).toList.map { n =>
        JmsTextMessage(n.toString).add("Number", n).add("IsOdd", n % 2 == 1).add("IsEven", n % 2 == 0)
      }
      //#create-messages-with-properties

      Source(msgsIn).runWith(jmsSink)

      //#create-jms-source
      val jmsSource: Source[Message, NotUsed] = JmsSource(
        JmsSourceSettings(connectionFactory).withBufferSize(10).withQueue("numbers")
      )
      //#create-jms-source

      //#run-jms-source
      val result = jmsSource.take(msgsIn.size).runWith(Sink.seq)
      //#run-jms-source

      // The sent message and the receiving one should have the same properties
      result.futureValue.zip(msgsIn).foreach {
        case (out, in) =>
          out.getIntProperty("Number") shouldEqual in.properties("Number")
          out.getBooleanProperty("IsOdd") shouldEqual in.properties("IsOdd")
          out.getBooleanProperty("IsEven") shouldEqual in.properties("IsEven")
      }
    }

    "publish JMS text messages with properties through a queue and consume them with a selector" in withServer() {
      ctx =>
        val url: String = ctx.url
        val connectionFactory = new ActiveMQConnectionFactory(url)

        val jmsSink: Sink[JmsTextMessage, NotUsed] = JmsSink(
          JmsSinkSettings(connectionFactory).withQueue("numbers")
        )

        val msgsIn = (1 to 10).toList.map { n =>
          JmsTextMessage(n.toString).add("Number", n).add("IsOdd", n % 2 == 1).add("IsEven", n % 2 == 0)
        }
        Source(msgsIn).runWith(jmsSink)

        //#create-jms-source-with-selector
        val jmsSource = JmsSource(
          JmsSourceSettings(connectionFactory).withBufferSize(10).withQueue("numbers").withSelector("IsOdd = TRUE")
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
      Source(in).runWith(JmsSink.textSink(JmsSinkSettings(connectionFactory).withQueue("test")))

      val result = JmsSource
        .textSource(JmsSourceSettings(connectionFactory).withBufferSize(1).withQueue("test"))
        .throttle(1, 1.second, 1, ThrottleMode.shaping)
        .take(in.size)
        .runWith(Sink.seq)

      result.futureValue shouldEqual in
    }

    "disconnection should fail the stage" in withServer() { ctx =>
      val connectionFactory = new ActiveMQConnectionFactory(ctx.url)
      val result = JmsSource(JmsSourceSettings(connectionFactory).withQueue("test")).runWith(Sink.seq)
      Thread.sleep(500)
      ctx.broker.stop()
      result.failed.futureValue shouldBe an[JMSException]
    }

    "publish and consume elements through a topic " in withServer() { ctx =>
      import system.dispatcher

      val connectionFactory = new ActiveMQConnectionFactory(ctx.url)

      //#create-topic-sink
      val jmsTopicSink: Sink[String, NotUsed] = JmsSink.textSink(
        JmsSinkSettings(connectionFactory).withTopic("topic")
      )
      //#create-topic-sink
      val jmsTopicSink2: Sink[String, NotUsed] = JmsSink.textSink(
        JmsSinkSettings(connectionFactory).withTopic("topic")
      )

      val in = List("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k")
      val inNumbers = (1 to 10).map(_.toString)

      //#create-topic-source
      val jmsTopicSource: Source[String, NotUsed] = JmsSource.textSource(
        JmsSourceSettings(connectionFactory).withBufferSize(10).withTopic("topic")
      )
      //#create-topic-source
      val jmsSource2: Source[String, NotUsed] = JmsSource.textSource(
        JmsSourceSettings(connectionFactory).withBufferSize(10).withTopic("topic")
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

  }
}
