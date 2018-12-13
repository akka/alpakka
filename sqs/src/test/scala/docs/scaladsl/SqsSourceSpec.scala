/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import java.util.concurrent.Executors

import akka.Done
import akka.stream.alpakka.sqs._
import akka.stream.alpakka.sqs.scaladsl.{DefaultTestContext, SqsSource}
import akka.stream.scaladsl.Sink
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.client.builder.ExecutorFactory
import com.amazonaws.services.sqs.model.{MessageAttributeValue, QueueDoesNotExistException, SendMessageRequest}
import com.amazonaws.services.sqs.{AmazonSQSAsync, AmazonSQSAsyncClientBuilder}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.concurrent.duration._

class SqsSourceSpec extends FlatSpec with ScalaFutures with Matchers with DefaultTestContext {

  implicit override val patienceConfig = PatienceConfig(timeout = 5.seconds, interval = 100.millis)

  trait IntegrationFixture {
    val queueUrl: String = randomQueueUrl()
    implicit val awsSqsClient: AmazonSQSAsync = sqsClient
  }

  "SqsSource" should "stream a single batch from the queue" taggedAs Integration in new IntegrationFixture {
    sqsClient.sendMessage(queueUrl, "alpakka")

    val future = SqsSource(queueUrl, SqsSourceSettings.Defaults)
      .runWith(Sink.head)

    future.futureValue.getBody shouldBe "alpakka"
  }

  it should "continue streaming if receives an empty response" taggedAs Integration in new IntegrationFixture {
    val future = SqsSource(queueUrl, SqsSourceSettings().withWaitTimeSeconds(0))
      .runWith(Sink.ignore)

    // make sure the source polled sqs once for an empty response
    Thread.sleep(1.second.toMillis)

    future.isCompleted shouldBe false
  }

  it should "terminate on an empty response if requested" taggedAs Integration in new IntegrationFixture {
    val future = SqsSource(queueUrl, SqsSourceSettings().withWaitTimeSeconds(0).withCloseOnEmptyReceive(true))
      .runWith(Sink.ignore)

    future.futureValue shouldBe Done
  }

  it should "finish immediately if the queue does not exist" taggedAs Integration in new IntegrationFixture {
    val notExistingQueue = s"$sqsEndpoint/queue/not-existing"

    val future = SqsSource(notExistingQueue, SqsSourceSettings.Defaults).runWith(Sink.seq)

    future.failed.futureValue shouldBe a[QueueDoesNotExistException]
  }

  it should "ask for all the attributes set in the settings" taggedAs Integration in new IntegrationFixture {
    val attributes = List(SentTimestamp, ApproximateReceiveCount, SenderId)
    val settings = SqsSourceSettings.Defaults.withAttributes(attributes)

    sqsClient.sendMessage(queueUrl, "alpakka")

    val future = SqsSource(queueUrl, settings).runWith(Sink.head)

    future.futureValue.getAttributes.keySet.asScala shouldBe attributes.map(_.name).toSet
  }

  it should "ask for all the message attributes set in the settings" taggedAs Integration in new IntegrationFixture {
    val messageAttributes = Map(
      "attribute-1" -> new MessageAttributeValue().withStringValue("v1").withDataType("String"),
      "attribute-2" -> new MessageAttributeValue().withStringValue("v2").withDataType("String")
    )
    val settings =
      SqsSourceSettings.Defaults.withMessageAttributes(messageAttributes.keys.toList.map(MessageAttributeName.apply))

    sqsClient.sendMessage(new SendMessageRequest(queueUrl, "alpakka").withMessageAttributes(messageAttributes.asJava))

    val future = SqsSource(queueUrl, settings)
      .runWith(Sink.head)

    future.futureValue.getMessageAttributes.asScala shouldBe messageAttributes
  }

  "SqsSourceSettings" should "be constructed" in {
    //#SqsSourceSettings
    val settings = SqsSourceSettings()
      .withWaitTime(20.seconds)
      .withMaxBufferSize(100)
      .withMaxBatchSize(10)
      .withAttributes(immutable.Seq(SenderId, SentTimestamp))
      .withMessageAttribute(MessageAttributeName.create("bar.*"))
      .withCloseOnEmptyReceive(true)
      .withVisibilityTimeout(10.seconds)
    //#SqsSourceSettings

    settings.maxBufferSize should be(100)
  }

  it should "accept valid parameters" in {
    val s = SqsSourceSettings()
      .withWaitTimeSeconds(1)
      .withMaxBatchSize(2)
      .withMaxBufferSize(3)
      .withAttribute(All)
    s.attributeNames should be(List(All))
  }

  it should "require maxBatchSize <= maxBufferSize" in {
    a[IllegalArgumentException] should be thrownBy {
      SqsSourceSettings().withMaxBatchSize(5).withMaxBufferSize(3)
    }
  }

  it should "require waitTimeSeconds within AWS SQS limits" in {
    a[IllegalArgumentException] should be thrownBy {
      SqsSourceSettings().withWaitTimeSeconds(-1)
    }

    a[IllegalArgumentException] should be thrownBy {
      SqsSourceSettings().withWaitTimeSeconds(100)
    }
  }

  it should "require maxBatchSize within AWS SQS limits" in {
    a[IllegalArgumentException] should be thrownBy {
      SqsSourceSettings().withMaxBatchSize(0)
    }

    a[IllegalArgumentException] should be thrownBy {
      SqsSourceSettings().withMaxBatchSize(11)
    }
  }

  "SqsSource" should "stream a single batch from the queue with custom client" taggedAs Integration in new IntegrationFixture {
    //#init-custom-client
    val credentialsProvider = new AWSStaticCredentialsProvider(new BasicAWSCredentials("x", "x"))
    implicit val customSqsClient: AmazonSQSAsync =
      AmazonSQSAsyncClientBuilder
        .standard()
        .withCredentials(credentialsProvider)
        .withExecutorFactory(new ExecutorFactory {
          override def newExecutor() = Executors.newFixedThreadPool(10)
        })
        .withEndpointConfiguration(new EndpointConfiguration(sqsEndpoint, "eu-central-1"))
        .build()
    //#init-custom-client

    customSqsClient.sendMessage(queueUrl, "alpakka")

    val future = SqsSource(queueUrl, SqsSourceSettings())(customSqsClient)
      .runWith(Sink.head)

    future.futureValue.getBody shouldBe "alpakka"
  }

  it should "stream multiple batches from the queue" taggedAs Integration in new IntegrationFixture {
    val input = for (i <- 1 to 100) yield s"alpakka-$i"
    input.foreach(m => sqsClient.sendMessage(queueUrl, m))

    val future =
      //#run
      SqsSource(
        queueUrl,
        SqsSourceSettings().withCloseOnEmptyReceive(true).withWaitTime(1.second)
      ).runWith(Sink.seq)
    //#run

    future.futureValue should have size 100
  }

  it should "stream single message twice from the queue when visibility timeout passed" taggedAs Integration in new IntegrationFixture {
    sqsClient.sendMessage(queueUrl, "alpakka")

    val future = SqsSource(queueUrl, SqsSourceSettings().withVisibilityTimeout(1.second))
      .takeWithin(1500.milliseconds)
      .runWith(Sink.seq)

    future.futureValue should have size 2
  }

  it should "stream single message once from the queue when visibility timeout did not pass" taggedAs Integration in new IntegrationFixture {
    sqsClient.sendMessage(queueUrl, "alpakka")

    val future = SqsSource(queueUrl, SqsSourceSettings().withVisibilityTimeout(3.seconds))
      .takeWithin(500.milliseconds)
      .runWith(Sink.seq)

    future.futureValue should have size 1
  }

}
