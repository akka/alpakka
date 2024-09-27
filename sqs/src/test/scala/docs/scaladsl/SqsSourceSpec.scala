/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import java.net.URI
import java.util.concurrent.TimeUnit

import akka.Done
import akka.stream.KillSwitches
import akka.stream.alpakka.sqs._
import akka.stream.alpakka.sqs.scaladsl.{DefaultTestContext, SqsSource}
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import akka.stream.scaladsl.{Keep, Sink}
import com.github.matsluni.akkahttpspi.AkkaHttpClient
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.{
  Message,
  MessageAttributeValue,
  MessageSystemAttributeName,
  QueueDoesNotExistException,
  SendMessageRequest
}

import scala.jdk.CollectionConverters._
import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration._

class SqsSourceSpec extends AnyFlatSpec with ScalaFutures with Matchers with DefaultTestContext with LogCapturing {

  import SqsSourceSpec._

  implicit override val patienceConfig: PatienceConfig = PatienceConfig(timeout = 10.seconds, interval = 100.millis)

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

    sqsClient.sendMessage(sendMessageRequest).get(2, TimeUnit.SECONDS)

    val future = SqsSource(queueUrl, sqsSourceSettings)
      .runWith(Sink.head)

    future.futureValue.body() shouldBe "alpakka"
  }

  it should "continue streaming if receives an empty response" taggedAs Integration in {
    new IntegrationFixture {
      val (switch, source) = SqsSource(queueUrl, SqsSourceSettings().withWaitTimeSeconds(0))
        .viaMat(KillSwitches.single)(Keep.right)
        .toMat(Sink.ignore)(Keep.both)
        .run()

      // make sure the source polled sqs once for an empty response
      Thread.sleep(1.second.toMillis)

      source shouldNot be(Symbol("completed"))
      switch.shutdown()
    }
  }

  it should "terminate on an empty response if requested" taggedAs Integration in {
    new IntegrationFixture {
      val future = SqsSource(queueUrl, sqsSourceSettings.withCloseOnEmptyReceive(true))
        .runWith(Sink.ignore)

      future.futureValue shouldBe Done
    }
  }

  it should "finish immediately if the queue does not exist" taggedAs Integration in new IntegrationFixture {
    val notExistingQueue = s"$sqsEndpoint/queue/not-existing"

    val future = SqsSource(notExistingQueue, sqsSourceSettings).runWith(Sink.seq)

    future.failed.futureValue.getCause shouldBe a[QueueDoesNotExistException]
  }

  "SqsSource" should "ask for 'All' attributes set in the settings" taggedAs Integration in {
    new IntegrationFixture {
      val attribute = All
      val settings = sqsSourceSettings.withAttribute(attribute)

      val sendMessageRequest =
        SendMessageRequest
          .builder()
          .queueUrl(queueUrl)
          .messageBody("alpakka")
          .build()

      sqsClient.sendMessage(sendMessageRequest).get(2, TimeUnit.SECONDS)

      val future = SqsSource(queueUrl, settings).runWith(Sink.head)

      private val message: Message = future.futureValue
      message.attributes().keySet.asScala should contain theSameElementsAs allAvailableAttributes
        .map(attr => MessageSystemAttributeName.fromValue(attr.name))
    }
  }

  allAvailableAttributes foreach { attribute =>
    it should s"ask for '${attribute.name}' set in the settings" taggedAs Integration in {
      new IntegrationFixture {
        val settings = sqsSourceSettings.withAttribute(attribute)

        val sendMessageRequest =
          SendMessageRequest
            .builder()
            .queueUrl(queueUrl)
            .messageBody("alpakka")
            .build()

        sqsClient.sendMessage(sendMessageRequest).get(2, TimeUnit.SECONDS)

        val future = SqsSource(queueUrl, settings).runWith(Sink.head)

        private val message: Message = future.futureValue
        message.attributes().keySet.asScala should contain only MessageSystemAttributeName.fromValue(attribute.name)
      }
    }
  }

  it should "ask for multiple attributes set in the settings" taggedAs Integration in {
    new IntegrationFixture {
      val attributes = allAvailableAttributes.filterNot(attr => attr == All || attr == MessageGroupId)
      val settings = sqsSourceSettings.withAttributes(attributes)

      val sendMessageRequest =
        SendMessageRequest
          .builder()
          .queueUrl(queueUrl)
          .messageBody("alpakka")
          .build()

      sqsClient.sendMessage(sendMessageRequest).get(2, TimeUnit.SECONDS)

      val future = SqsSource(queueUrl, settings).runWith(Sink.head)

      private val message: Message = future.futureValue
      message.attributes().keySet.asScala should contain theSameElementsAs attributes
        .map(_.name)
        .map(MessageSystemAttributeName.fromValue)
    }
  }

  it should "ask for all the message attributes set in the settings" taggedAs Integration in {
    new IntegrationFixture {
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

      sqsClient.sendMessage(sendMessageRequest).get(2, TimeUnit.SECONDS)

      val future = SqsSource(queueUrl, settings)
        .runWith(Sink.head)

      future.futureValue.messageAttributes().asScala shouldBe messageAttributes
    }
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

  "SqsSource" should "stream a single batch from the queue with custom client" taggedAs Integration in {
    new IntegrationFixture {
      /*
    // #init-custom-client
    import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
    val customClient: SdkAsyncHttpClient = NettyNioAsyncHttpClient.builder().maxConcurrency(100).build()
    // #init-custom-client
       */
      val customClient: SdkAsyncHttpClient = AkkaHttpClient.builder().withActorSystem(system).build()

      //#init-custom-client
      implicit val customSqsClient: SqsAsyncClient = SqsAsyncClient
        .builder()
        .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("x", "x")))
        //#init-custom-client
        .endpointOverride(URI.create(sqsEndpoint))
        //#init-custom-client
        .region(Region.EU_CENTRAL_1)
        .httpClient(customClient)
        .build()

      //#init-custom-client

      val sendMessageRequest =
        SendMessageRequest
          .builder()
          .queueUrl(queueUrl)
          .messageBody("alpakka")
          .build()

      customSqsClient.sendMessage(sendMessageRequest).get(2, TimeUnit.SECONDS)

      val future = SqsSource(queueUrl, sqsSourceSettings)(customSqsClient)
        .runWith(Sink.head)

      future.futureValue.body() shouldBe "alpakka"
    }
  }

  it should "stream multiple batches from the queue" taggedAs Integration in {
    new IntegrationFixture {
      val input =
        for (i <- 1 to 100)
          yield SendMessageRequest
            .builder()
            .queueUrl(queueUrl)
            .messageBody(s"alpakka-$i")
            .build()

      input.foreach(m => sqsClient.sendMessage(m).get(2, TimeUnit.SECONDS))

      //#run
      val messages: Future[immutable.Seq[Message]] =
        SqsSource(
          queueUrl,
          SqsSourceSettings().withCloseOnEmptyReceive(true).withWaitTime(10.millis)
        ).runWith(Sink.seq)
      //#run

      messages.futureValue should have size 100
    }
  }

  it should "stream single message at least twice from the queue when visibility timeout passed" taggedAs Integration in {
    new IntegrationFixture {
      val sendMessageRequest =
        SendMessageRequest
          .builder()
          .queueUrl(queueUrl)
          .messageBody("alpakka")
          .build()

      sqsClient.sendMessage(sendMessageRequest).get(2, TimeUnit.SECONDS)

      val future = SqsSource(queueUrl, sqsSourceSettings.withVisibilityTimeout(1.second))
        .takeWithin(1200.milliseconds)
        .runWith(Sink.seq)

      future.futureValue.size should be > 1
    }
  }

  it should "stream single message once from the queue when visibility timeout did not pass" taggedAs Integration in {
    new IntegrationFixture {
      val sendMessageRequest =
        SendMessageRequest
          .builder()
          .queueUrl(queueUrl)
          .messageBody("alpakka")
          .build()

      sqsClient.sendMessage(sendMessageRequest).get(2, TimeUnit.SECONDS)

      val future = SqsSource(queueUrl, sqsSourceSettings.withVisibilityTimeout(10.seconds))
        .takeWithin(500.milliseconds)
        .runWith(Sink.seq)

      future.futureValue should have size 1
    }
  }
}

object SqsSourceSpec {
  private val allAvailableAttributes = List(
    ApproximateFirstReceiveTimestamp,
    ApproximateReceiveCount,
    SenderId,
    SentTimestamp
    // MessageDeduplicationId, removed from ElasticMq 1.3.4
    // MessageGroupId, removed from ElasticMq 1.3.4
    // SequenceNumber, not supported by elasticmq
  )
}
