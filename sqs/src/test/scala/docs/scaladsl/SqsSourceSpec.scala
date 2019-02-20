/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import java.net.URI

import akka.Done
import akka.stream.alpakka.sqs._
import akka.stream.alpakka.sqs.scaladsl.{DefaultTestContext, SqsSource}
import akka.stream.scaladsl.Sink
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FlatSpec, Matchers}
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.{
  Message,
  MessageAttributeValue,
  QueueDoesNotExistException,
  SendMessageRequest
}

import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.concurrent.duration._

class SqsSourceSpec extends FlatSpec with ScalaFutures with Matchers with DefaultTestContext {

  implicit override val patienceConfig = PatienceConfig(timeout = 10.seconds, interval = 100.millis)

  trait IntegrationFixture {
    val queueUrl: String = randomQueueUrl()
    implicit val awsSqsClient: SqsAsyncClient = sqsClient
  }

  "SqsSource" should "stream a single batch from the queue" taggedAs Integration in new IntegrationFixture {
    val sendMessageRequest =
      SendMessageRequest
        .builder()
        .queueUrl(queueUrl)
        .messageBody("alpakka")
        .build()

    sqsClient.sendMessage(sendMessageRequest).get()

    val future = SqsSource(queueUrl, sqsSourceSettings)
      .runWith(Sink.head)

    future.futureValue.body() shouldBe "alpakka"
  }

  it should "continue streaming if receives an empty response" taggedAs Integration in new IntegrationFixture {
    val future = SqsSource(queueUrl, SqsSourceSettings().withWaitTimeSeconds(0))
      .runWith(Sink.ignore)

    // make sure the source polled sqs once for an empty response
    Thread.sleep(1.second.toMillis)

    future.isCompleted shouldBe false
  }

  it should "terminate on an empty response if requested" taggedAs Integration in new IntegrationFixture {
    val future = SqsSource(queueUrl, sqsSourceSettings.withCloseOnEmptyReceive(true))
      .runWith(Sink.ignore)

    future.futureValue shouldBe Done
  }

  it should "finish immediately if the queue does not exist" taggedAs Integration in new IntegrationFixture {
    val notExistingQueue = s"$sqsEndpoint/queue/not-existing"

    val future = SqsSource(notExistingQueue, sqsSourceSettings).runWith(Sink.seq)

    future.failed.futureValue.getCause shouldBe a[QueueDoesNotExistException]
  }

  //TODO: it semms that the attribute names have changed a bit in the new SDK and at least with ElasticMQ it is not working properly
  ignore should "ask for all the attributes set in the settings" taggedAs Integration in new IntegrationFixture {
    val attributes = List(All, VisibilityTimeout, MaximumMessageSize, LastModifiedTimestamp)
    val settings = sqsSourceSettings.withAttributes(attributes)

    val sendMessageRequest =
      SendMessageRequest
        .builder()
        .queueUrl(queueUrl)
        .messageBody("alpakka")
        .build()

    sqsClient.sendMessage(sendMessageRequest).get()

    val future = SqsSource(queueUrl, settings).runWith(Sink.head)

    private val value: Message = future.futureValue
    value.attributes().keySet.asScala shouldBe attributes.map(_.name).toSet
  }

  it should "ask for all the message attributes set in the settings" taggedAs Integration in new IntegrationFixture {
    val messageAttributes = Map(
      "attribute-1" -> MessageAttributeValue.builder().stringValue("v1").dataType("String").build(),
      "attribute-2" -> MessageAttributeValue.builder().stringValue("v2").dataType("String").build()
    )
    val settings =
      sqsSourceSettings.withMessageAttributes(messageAttributes.keys.toList.map(MessageAttributeName.apply))

    val sendMessageRequest =
      SendMessageRequest
        .builder()
        .queueUrl(queueUrl)
        .messageBody("alpakka")
        .messageAttributes(messageAttributes.asJava)
        .build()

    sqsClient.sendMessage(sendMessageRequest).get()

    val future = SqsSource(queueUrl, settings)
      .runWith(Sink.head)

    future.futureValue.messageAttributes().asScala shouldBe messageAttributes
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
    implicit val customSqsClient = SqsAsyncClient
      .builder()
      .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("x", "x")))
      .endpointOverride(URI.create(sqsEndpoint))
      .region(Region.EU_CENTRAL_1)
      .build()

    //#init-custom-client

    val sendMessageRequest =
      SendMessageRequest
        .builder()
        .queueUrl(queueUrl)
        .messageBody("alpakka")
        .build()

    customSqsClient.sendMessage(sendMessageRequest).get()

    val future = SqsSource(queueUrl, sqsSourceSettings)(customSqsClient)
      .runWith(Sink.head)

    future.futureValue.body() shouldBe "alpakka"
  }

  it should "stream multiple batches from the queue" taggedAs Integration in new IntegrationFixture {
    val input = for (i <- 1 to 100)
      yield
        SendMessageRequest
          .builder()
          .queueUrl(queueUrl)
          .messageBody(s"alpakka-$i")
          .build()

    input.foreach(m => sqsClient.sendMessage(m).get())

    val future =
      //#run
      SqsSource(
        queueUrl,
        SqsSourceSettings().withCloseOnEmptyReceive(true).withWaitTime(0.second)
      ).runWith(Sink.seq)
    //#run

    future.futureValue should have size 100
  }

  it should "stream single message at least twice from the queue when visibility timeout passed" taggedAs Integration in new IntegrationFixture {
    val sendMessageRequest =
      SendMessageRequest
        .builder()
        .queueUrl(queueUrl)
        .messageBody("alpakka")
        .build()

    sqsClient.sendMessage(sendMessageRequest).get()

    val future = SqsSource(queueUrl, sqsSourceSettings.withVisibilityTimeout(1.second))
      .takeWithin(1200.milliseconds)
      .runWith(Sink.seq)

    future.futureValue.size should be > 1
  }

  it should "stream single message once from the queue when visibility timeout did not pass" taggedAs Integration in new IntegrationFixture {
    val sendMessageRequest =
      SendMessageRequest
        .builder()
        .queueUrl(queueUrl)
        .messageBody("alpakka")
        .build()

    sqsClient.sendMessage(sendMessageRequest).get()

    val future = SqsSource(queueUrl, sqsSourceSettings.withVisibilityTimeout(5.seconds))
      .takeWithin(500.milliseconds)
      .runWith(Sink.seq)

    future.futureValue should have size 1
  }

}
