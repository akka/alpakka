/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import java.util.concurrent.Executors

import akka.stream.alpakka.sqs._
import akka.stream.alpakka.sqs.scaladsl.{DefaultTestContext, SqsSource}
import akka.stream.scaladsl.Sink
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.client.builder.ExecutorFactory
import com.amazonaws.services.sqs.model.Message
import com.amazonaws.services.sqs.{AmazonSQSAsync, AmazonSQSAsyncClientBuilder}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{AsyncWordSpec, Matchers}

import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration._

class SqsSourceSnippetsSpec extends AsyncWordSpec with ScalaFutures with Matchers with DefaultTestContext {

  implicit val defaultPatience =
    PatienceConfig(timeout = 5.seconds, interval = 100.millis)

  "SqsSourceSettings" should {
    "be constructed" in {
      //#SqsSourceSettings
      val settings = SqsSourceSettings()
        .withWaitTime(20.seconds)
        .withMaxBufferSize(100)
        .withMaxBatchSize(10)
        .withAttributes(immutable.Seq(SenderId, SentTimestamp))
        .withMessageAttribute(MessageAttributeName.create("bar.*"))
        .withCloseOnEmptyReceive(true)
      //#SqsSourceSettings

      settings.maxBufferSize should be(100)
    }

    "accept valid parameters" in {
      val s = SqsSourceSettings(waitTimeSeconds = 1, maxBatchSize = 2, maxBufferSize = 3)
        .withAttribute(All)
      s.attributeNames should be(List(All))
    }

    "require maxBatchSize <= maxBufferSize" in {
      a[IllegalArgumentException] should be thrownBy {
        SqsSourceSettings(waitTimeSeconds = 1, maxBatchSize = 5, maxBufferSize = 3)
      }
    }

    "require waitTimeSeconds within AWS SQS limits" in {
      a[IllegalArgumentException] should be thrownBy {
        SqsSourceSettings(waitTimeSeconds = -1, maxBatchSize = 1, maxBufferSize = 2)
      }

      a[IllegalArgumentException] should be thrownBy {
        SqsSourceSettings(waitTimeSeconds = 100, maxBatchSize = 1, maxBufferSize = 2)
      }
    }

    "require maxBatchSize within AWS SQS limits" in {
      a[IllegalArgumentException] should be thrownBy {
        SqsSourceSettings(waitTimeSeconds = 5, maxBatchSize = 0, maxBufferSize = 2)
      }

      a[IllegalArgumentException] should be thrownBy {
        SqsSourceSettings(waitTimeSeconds = 5, maxBatchSize = 11, maxBufferSize = 2)
      }
    }

  }

  "SqsSource" should {

    "stream a single batch from the queue with custom client" taggedAs Integration in {
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

      val queue = randomQueueUrl()
      customSqsClient.sendMessage(queue, "alpakka")

      SqsSource(queue, SqsSourceSettings())(customSqsClient)
        .take(1)
        .runWith(Sink.head)
        .map(_.getBody shouldBe "alpakka")
    }

    "stream multiple batches from the queue" taggedAs Integration in {

      val queue = randomQueueUrl()
      implicit val awsSqsClient = sqsClient

      val input = 1 to 100 map { i =>
        s"alpakka-$i"
      }

      input foreach { m =>
        sqsClient.sendMessage(queue, m)
      }
      //#run
      val res: Future[immutable.Seq[Message]] =
        SqsSource(
          queue,
          SqsSourceSettings().withCloseOnEmptyReceive(true).withWaitTime(1.second)
        ).runWith(Sink.seq)
      //#run

      res.futureValue should have size 100
    }
  }
}
