/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs.scaladsl

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import com.amazonaws.services.sqs.AmazonSQSAsync
import com.amazonaws.services.sqs.model.CreateQueueRequest
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Suite, Tag}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Random

trait DefaultTestContext extends BeforeAndAfterAll with ScalaFutures { this: Suite =>

  //#init-mat
  implicit val system: ActorSystem = ActorSystem()
  implicit val mat: Materializer = ActorMaterializer()
  //#init-mat

  // endpoint of the elasticmq docker container
  val sqsEndpoint: String = "http://localhost:9324"

  object Integration extends Tag("akka.stream.alpakka.sqs.scaladsl.Integration")

  def sqsClient = createAsyncClient(sqsEndpoint)

  def randomQueueUrl(): String = sqsClient.createQueue(s"queue-${Random.nextInt}").getQueueUrl

  val fifoQueueRequest = new CreateQueueRequest(s"queue-${Random.nextInt}.fifo")
    .addAttributesEntry("FifoQueue", "true")
    .addAttributesEntry("ContentBasedDeduplication", "true")

  def randomFifoQueueUrl(): String = sqsClient.createQueue(fifoQueueRequest).getQueueUrl

  override protected def afterAll(): Unit =
    try {
      Await.ready(system.terminate(), 5.seconds)
    } finally {
      super.afterAll()
    }

  def createAsyncClient(sqsEndpoint: String): AmazonSQSAsync = {
    //#init-client
    import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
    import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
    import com.amazonaws.services.sqs.{AmazonSQSAsync, AmazonSQSAsyncClientBuilder}

    val credentialsProvider = new AWSStaticCredentialsProvider(new BasicAWSCredentials("x", "x"))
    implicit val awsSqsClient: AmazonSQSAsync = AmazonSQSAsyncClientBuilder
      .standard()
      .withCredentials(credentialsProvider)
      .withEndpointConfiguration(new EndpointConfiguration(sqsEndpoint, "eu-central-1"))
      .build()
    system.registerOnTermination(awsSqsClient.shutdown())
    //#init-client
    awsSqsClient
  }
}
