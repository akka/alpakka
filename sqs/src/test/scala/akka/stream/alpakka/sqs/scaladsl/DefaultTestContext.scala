/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs.scaladsl

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.amazonaws.auth.{AWSCredentialsProvider, AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.sqs.{AmazonSQSAsync, AmazonSQSAsyncClientBuilder}
import org.elasticmq.rest.sqs.{SQSLimits, SQSRestServer, SQSRestServerBuilder}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite, Tag}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Random

trait DefaultTestContext extends BeforeAndAfterAll with BeforeAndAfterEach { this: Suite =>

  //#init-mat
  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()
  //#init-mat

  var sqsServer: SQSRestServer = _
  def sqsAddress = sqsServer.waitUntilStarted().localAddress
  def sqsPort = sqsAddress.getPort
  def sqsEndpoint: String =
    s"http://${sqsAddress.getHostName}:$sqsPort"

  object Integration extends Tag("akka.stream.alpakka.sqs.scaladsl.Integration")

  val credentialsProvider = new AWSStaticCredentialsProvider(new BasicAWSCredentials("x", "x"))

  implicit def sqsClient = createAsyncClient(sqsEndpoint, credentialsProvider)

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

  def createAsyncClient(sqsEndpoint: String, credentialsProvider: AWSCredentialsProvider): AmazonSQSAsync = {
    //#init-client
    val client: AmazonSQSAsync = AmazonSQSAsyncClientBuilder
      .standard()
      .withCredentials(credentialsProvider)
      .withEndpointConfiguration(new EndpointConfiguration(sqsEndpoint, "eu-central-1"))
      .build()
    //#init-client
    client
  }

}
