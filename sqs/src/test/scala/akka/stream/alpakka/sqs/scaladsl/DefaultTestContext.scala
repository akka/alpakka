/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs.scaladsl

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import com.amazonaws.services.sqs.AmazonSQSAsync
import org.elasticmq.rest.sqs.{SQSLimits, SQSRestServer, SQSRestServerBuilder}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite, Tag}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Random

trait DefaultTestContext extends BeforeAndAfterAll with BeforeAndAfterEach with ScalaFutures { this: Suite =>

  //#init-mat
  implicit val system: ActorSystem = ActorSystem()
  implicit val mat: Materializer = ActorMaterializer()
  //#init-mat

  var sqsServer: SQSRestServer = _
  def sqsAddress = sqsServer.waitUntilStarted().localAddress
  def sqsEndpoint: String =
    s"http://${sqsAddress.getHostName}:${sqsAddress.getPort}"

  object Integration extends Tag("akka.stream.alpakka.sqs.scaladsl.Integration")

  def sqsClient = createAsyncClient(sqsEndpoint)

  def randomQueueUrl(): String = sqsClient.createQueue(s"queue-${Random.nextInt}").getQueueUrl

  override protected def beforeEach(): Unit =
    sqsServer = SQSRestServerBuilder.withActorSystem(system).withSQSLimits(SQSLimits.Relaxed).withDynamicPort().start()

  override protected def afterEach(): Unit =
    sqsServer.stopAndWait()

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
