/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs.scaladsl

import java.util.concurrent.Executors

import akka.stream.alpakka.sqs._
import akka.stream.scaladsl.Sink
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.client.builder.ExecutorFactory
import com.amazonaws.services.sqs.model.{MessageAttributeValue, QueueDoesNotExistException, SendMessageRequest}
import com.amazonaws.services.sqs.{AmazonSQSAsync, AmazonSQSAsyncClientBuilder}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{AsyncWordSpec, Matchers}

import scala.collection.JavaConverters._

class SqsSourceSpec extends AsyncWordSpec with ScalaFutures with Matchers with DefaultTestContext {

  private val sqsSourceSettings = SqsSourceSettings.Defaults

  "SqsSource" should {

    "stream a single batch from the queue" taggedAs Integration in {

      val queue = randomQueueUrl()
      sqsClient.sendMessage(queue, "alpakka")

      SqsSource(queue, sqsSourceSettings).take(1).runWith(Sink.head).map(_.getBody shouldBe "alpakka")

    }

    "stream a single batch from the queue with custom client" taggedAs Integration in {
      //#init-custom-client
      val customSqsClient: AmazonSQSAsync =
        AmazonSQSAsyncClientBuilder
          .standard()
          .withCredentials(credentialsProvider)
          .withExecutorFactory(new ExecutorFactory {
            override def newExecutor() = Executors.newFixedThreadPool(10)
          })
          .withEndpointConfiguration(new EndpointConfiguration(sqsEndpoint, "eu-central-1"))
          .build()
      //#init-custom-client

      val queue = randomQueueUrl()
      customSqsClient.sendMessage(queue, "alpakka")

      SqsSource(queue, sqsSourceSettings)(customSqsClient).take(1).runWith(Sink.head).map(_.getBody shouldBe "alpakka")
    }

    "stream multiple batches from the queue" taggedAs Integration in {

      val queue = randomQueueUrl()

      val input = 1 to 100 map { i =>
        s"alpakka-$i"
      }

      input foreach { m =>
        sqsClient.sendMessage(queue, m)
      }

      //#run
      SqsSource(queue, sqsSourceSettings).take(100).runWith(Sink.seq).map(_ should have size 100)
      //#run

    }

    "continue streaming if receives an empty response" taggedAs Integration in {

      val queue = randomQueueUrl()

      val f = SqsSource(queue, SqsSourceSettings(0, 100, 10)).take(1).runWith(Sink.seq)

      sqsClient.sendMessage(queue, "alpakka")

      f.map(_ should have size 1)
    }

    "finish immediately if the queue does not exist" taggedAs Integration in {

      val queue = s"$sqsEndpoint/queue/not-existing"

      val f = SqsSource(queue, sqsSourceSettings).runWith(Sink.seq)

      f.failed.map(_ shouldBe a[QueueDoesNotExistException])
    }

    "ask for all the attributes set in the settings" taggedAs Integration in {
      val queue = randomQueueUrl()
      val attributes = List(SentTimestamp, ApproximateReceiveCount, SenderId)
      val settings = sqsSourceSettings.copy(attributeNames = attributes)

      sqsClient.sendMessage(queue, "alpakka")

      SqsSource(queue, settings)
        .take(1)
        .runWith(Sink.head)
        .map(_.getAttributes.keySet.asScala shouldBe attributes.map(_.name).toSet)
    }

    "ask for all the message attributes set in the settings" taggedAs Integration in {
      val queue = randomQueueUrl()
      val messageAttributes = Map(
        "attribute-1" -> new MessageAttributeValue().withStringValue("v1").withDataType("String"),
        "attribute-2" -> new MessageAttributeValue().withStringValue("v2").withDataType("String")
      )
      val settings =
        sqsSourceSettings.copy(messageAttributeNames = messageAttributes.keys.toList.map(MessageAttributeName))

      sqsClient.sendMessage(new SendMessageRequest(queue, "alpakka").withMessageAttributes(messageAttributes.asJava))

      SqsSource(queue, settings)
        .take(1)
        .runWith(Sink.head)
        .map(_.getMessageAttributes.asScala shouldBe messageAttributes)
    }
  }
}
