/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs.scaladsl

import akka.stream.alpakka.sqs._
import akka.stream.scaladsl.Sink
import com.amazonaws.services.sqs.model.{MessageAttributeValue, QueueDoesNotExistException, SendMessageRequest}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{AsyncWordSpec, Matchers}

import scala.collection.JavaConverters._

class SqsSourceSpec extends AsyncWordSpec with ScalaFutures with Matchers with DefaultTestContext {

  private val sqsSourceSettings = SqsSourceSettings.Defaults

  "SqsSource" should {

    "stream a single batch from the queue" taggedAs Integration in {
      val queue = randomQueueUrl()
      implicit val awsSqsClient = sqsClient
      sqsClient.sendMessage(queue, "alpakka")

      SqsSource(queue, sqsSourceSettings).take(1).runWith(Sink.head).map(_.getBody shouldBe "alpakka")

    }

    "continue streaming if receives an empty response" taggedAs Integration in {
      val queue = randomQueueUrl()
      implicit val awsSqsClient = sqsClient

      val f = SqsSource(queue, SqsSourceSettings(0, 100, 10)).take(1).runWith(Sink.seq)

      sqsClient.sendMessage(queue, "alpakka")

      f.map(_ should have size 1)
    }

    "terminate on an empty response if requested" taggedAs Integration in {
      val queue = randomQueueUrl()
      implicit val awsSqsClient = sqsClient

      sqsClient.sendMessage(queue, "alpakka")
      val f = SqsSource(queue, SqsSourceSettings(0, 100, 10, closeOnEmptyReceive = true)).runWith(Sink.seq)

      f.map(_ should have size 1)
    }

    "finish immediately if the queue does not exist" taggedAs Integration in {
      val queue = s"$sqsEndpoint/queue/not-existing"
      implicit val awsSqsClient = sqsClient

      val f = SqsSource(queue, sqsSourceSettings).runWith(Sink.seq)

      f.failed.map(_ shouldBe a[QueueDoesNotExistException])
    }

    "ask for all the attributes set in the settings" taggedAs Integration in {
      val queue = randomQueueUrl()
      implicit val awsSqsClient = sqsClient

      val attributes = List(SentTimestamp, ApproximateReceiveCount, SenderId)
      val settings = sqsSourceSettings.withAttributes(attributes)

      sqsClient.sendMessage(queue, "alpakka")

      SqsSource(queue, settings)
        .take(1)
        .runWith(Sink.head)
        .map(_.getAttributes.keySet.asScala shouldBe attributes.map(_.name).toSet)
    }

    "ask for all the message attributes set in the settings" taggedAs Integration in {
      val queue = randomQueueUrl()
      implicit val awsSqsClient = sqsClient
      val messageAttributes = Map(
        "attribute-1" -> new MessageAttributeValue().withStringValue("v1").withDataType("String"),
        "attribute-2" -> new MessageAttributeValue().withStringValue("v2").withDataType("String")
      )
      val settings =
        sqsSourceSettings.withMessageAttributes(messageAttributes.keys.toList.map(MessageAttributeName.apply))

      sqsClient.sendMessage(new SendMessageRequest(queue, "alpakka").withMessageAttributes(messageAttributes.asJava))

      SqsSource(queue, settings)
        .take(1)
        .runWith(Sink.head)
        .map(_.getMessageAttributes.asScala shouldBe messageAttributes)
    }
  }
}
