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
import com.amazonaws.services.sqs.model.{Message, MessageAttributeValue, QueueDoesNotExistException, SendMessageRequest}
import com.amazonaws.services.sqs.{AmazonSQSAsync, AmazonSQSAsyncClientBuilder}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{AsyncWordSpec, Matchers}

import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.concurrent.Future

class SourceSnippetsSpec extends AsyncWordSpec with ScalaFutures with Matchers with DefaultTestContext {

  private val sqsSourceSettings = SqsSourceSettings.Defaults

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

      SqsSource(queue, sqsSourceSettings)(customSqsClient).take(1).runWith(Sink.head).map(_.getBody shouldBe "alpakka")
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
        SqsSource(queue, sqsSourceSettings)
          .take(100)
          .runWith(Sink.seq)
      //#run

      res.futureValue should have size 100
    }
  }
}
