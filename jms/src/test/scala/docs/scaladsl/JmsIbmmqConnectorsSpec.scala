/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import akka.Done
import akka.stream.alpakka.jms._
import akka.stream.alpakka.jms.scaladsl.{JmsConsumer, JmsConsumerControl, JmsProducer}
import akka.stream.scaladsl.{Sink, Source}
import com.ibm.mq.jms.{MQQueueConnectionFactory, MQQueueSession, MQTopicConnectionFactory}
import com.ibm.msg.client.wmq.common.CommonConstants
import javax.jms.{Session, TextMessage}
import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.Future

class JmsIbmmqConnectorsSpec extends JmsSpec {
  override implicit val patienceConfig = PatienceConfig(2.minutes)

  "The JMS Ibmmq Connectors" should {
    val queueConnectionFactory = {
      //#ibmmq-connection-factory
      // Create the IBM MQ MQQueueConnectionFactory
      val connectionFactory = new MQQueueConnectionFactory()

      // align to docker image: ibmcom/mq:9.1.1.0
      connectionFactory.setHostName("localhost")
      connectionFactory.setPort(1414)
      connectionFactory.setQueueManager("QM1")
      connectionFactory.setChannel("DEV.APP.SVRCONN")

      //#ibmmq-connection-factory

      connectionFactory
    }

    "publish and consume strings through a queue" in {

      //#ibmmq-queue
      // Connect to IBM MQ over TCP/IP
      queueConnectionFactory.setTransportType(CommonConstants.WMQ_CM_CLIENT)
      val queueName = "DEV.QUEUE.1"

      val jmsSink: Sink[String, Future[Done]] = JmsProducer.textSink(
        JmsProducerSettings(producerConfig, queueConnectionFactory)
          .withQueue(queueName)
      )

      //#ibmmq-queue

      val in = 0 to 25 map (i => ('a' + i).asInstanceOf[Char].toString)
      Source(in).runWith(jmsSink)

      //#ibmmq-queue
      // Option1: create Source using default factory with just name
      val jmsSource: Source[TxEnvelope, JmsConsumerControl] = JmsConsumer.txSource(
        JmsConsumerSettings(consumerConfig, queueConnectionFactory)
          .withQueue(queueName)
      )

      //#ibmmq-queue

      val result = jmsSource
        .take(in.size)
        .map(env => (env, env.message.asInstanceOf[TextMessage].getText))
        .map { case (env, text) => env.commit(); text }
        .runWith(Sink.seq)

      result.futureValue should contain theSameElementsAs in
    }

    "publish and consume strings through a queue with custom destination" in {

      queueConnectionFactory.setTransportType(CommonConstants.WMQ_CM_CLIENT)
      //#ibmmq-custom-destination
      // Option2: create Source using custom factory
      val customQueue = "DEV.QUEUE.3"
      //#ibmmq-custom-destination

      val jmsSink: Sink[String, Future[Done]] = JmsProducer.textSink(
        JmsProducerSettings
          .create(system, queueConnectionFactory)
          .withDestination(CustomDestination("custom", createQueue(customQueue)))
      )

      val in = 0 to 25 map (i => ('a' + i).asInstanceOf[Char].toString)

      val streamCompletion: Future[Done] = Source(in).runWith(jmsSink)

      //#ibmmq-custom-destination
      val jmsSource: Source[String, JmsConsumerControl] = JmsConsumer.textSource(
        JmsConsumerSettings
          .create(system, queueConnectionFactory)
          .withDestination(CustomDestination("custom", createQueue(customQueue)))
      )

      //#ibmmq-custom-destination

      val result: Future[immutable.Seq[String]] = jmsSource.take(in.size).runWith(Sink.seq)

      streamCompletion.futureValue shouldEqual Done
      result.futureValue shouldEqual in
    }
    //#ibmmq-custom-destination
    def createQueue(destinationName: String): Session => javax.jms.Queue = { (session: Session) =>
      // cast to correct session implementation: MQQueueSession, MQTopicSession, MQSession
      val mqSession = session.asInstanceOf[MQQueueSession]
      mqSession.createQueue(destinationName)
    }
    //#ibmmq-custom-destination

    "publish and consume strings through a topic" in {
      // Create the IBM MQ MQTopicConnectionFactory
      val topicConnectionFactory = new MQTopicConnectionFactory()
      topicConnectionFactory.setHostName("localhost")
      topicConnectionFactory.setPort(1414)
      topicConnectionFactory.setQueueManager("QM1")
      topicConnectionFactory.setChannel("DEV.APP.SVRCONN")

      //#ibmmq-topic
      // Connect to IBM MQ over TCP/IP
      topicConnectionFactory.setTransportType(CommonConstants.WMQ_CM_CLIENT)
      val testTopicName = "dev/"

      val jmsTopicSink: Sink[String, Future[Done]] = JmsProducer.textSink(
        JmsProducerSettings
          .create(system, topicConnectionFactory)
          .withTopic(testTopicName)
      )

      // Option1: create Source using default factory with just name
      val jmsTopicSource: Source[String, JmsConsumerControl] = JmsConsumer
        .textSource(
          JmsConsumerSettings
            .create(system, topicConnectionFactory)
            .withTopic(testTopicName)
        )

      //#ibmmq-topic

      val in = 0 to 25 map (i => ('a' + i).asInstanceOf[Char].toString)

      val result: Future[immutable.Seq[String]] = jmsTopicSource.take(in.size).runWith(Sink.seq)

      Thread.sleep(500)

      val streamCompletion: Future[Done] = Source(in).runWith(jmsTopicSink)

      streamCompletion.futureValue shouldEqual Done
      result.futureValue shouldEqual in
    }
  }

}
