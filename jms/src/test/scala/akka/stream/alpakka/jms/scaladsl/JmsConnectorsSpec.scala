/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.jms.scaladsl

import javax.jms.JMSException

import akka.NotUsed
import akka.stream.ThrottleMode
import akka.stream.alpakka.jms.{JmsSinkSettings, JmsSourceSettings, JmsSpec}
import akka.stream.scaladsl.{Sink, Source}
import org.apache.activemq.ActiveMQConnectionFactory

import scala.concurrent.duration._

class JmsConnectorsSpec extends JmsSpec {

  override implicit val patienceConfig = PatienceConfig(1.minute)

  "The JMS Connectors" should {
    "publish and consume elements through a queue" in withServer() { ctx =>
      val url: String = ctx.url
      //#connection-factory
      val connectionFactory = new ActiveMQConnectionFactory(url)
      //#connection-factory

      //#create-sink
      val jmsSink: Sink[String, NotUsed] = JmsSink(
        JmsSinkSettings(connectionFactory).withQueue("test")
      )
      //#create-sink

      //#run-sink
      val in = List("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k")
      Source(in).runWith(jmsSink)
      //#run-sink

      //#create-source
      val jmsSource: Source[String, NotUsed] = JmsSource.textSource(
        JmsSourceSettings(connectionFactory).withBufferSize(10).withQueue("test")
      )
      //#create-source

      //#run-source
      val result = jmsSource.take(in.size).runWith(Sink.seq)
      //#run-source

      result.futureValue shouldEqual in
    }

    "applying backpressure when the consumer is slower than the producer" in withServer() { ctx =>
      val connectionFactory = new ActiveMQConnectionFactory(ctx.url)
      val in = List("a", "b", "c")
      Source(in).runWith(JmsSink(JmsSinkSettings(connectionFactory).withQueue("test")))

      val result = JmsSource
        .textSource(JmsSourceSettings(connectionFactory).withBufferSize(1).withQueue("test"))
        .throttle(1, 1.second, 1, ThrottleMode.shaping)
        .take(in.size)
        .runWith(Sink.seq)

      result.futureValue shouldEqual in
    }

    "deconnection should fail the stage" in withServer() { ctx =>
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
      val jmsTopicSink: Sink[String, NotUsed] = JmsSink(
        JmsSinkSettings(connectionFactory).withTopic("topic")
      )
      //#create-topic-sink
      val jmsTopicSink2: Sink[String, NotUsed] = JmsSink(
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
