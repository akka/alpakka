/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.sqs.scaladsl

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.amazonaws.auth.{AWSCredentialsProvider, AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.sqs.{AmazonSQSAsync, AmazonSQSAsyncClientBuilder}
import org.elasticmq.rest.sqs.{SQSRestServer, SQSRestServerBuilder}
import org.scalatest.{BeforeAndAfterAll, Suite, Tag}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Random

trait DefaultTestContext extends BeforeAndAfterAll { this: Suite =>

  lazy val sqsServer: SQSRestServer = SQSRestServerBuilder.withDynamicPort().start()
  lazy val sqsAddress = sqsServer.waitUntilStarted().localAddress
  lazy val sqsPort = sqsAddress.getPort
  lazy val sqsEndpoint: String = {
    s"http://${sqsAddress.getHostName}:$sqsPort"
  }

  object Integration extends Tag("akka.stream.alpakka.sqs.scaladsl.Integration")

  //#init-mat
  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()
  //#init-mat

  val credentialsProvider = new AWSStaticCredentialsProvider(new BasicAWSCredentials("x", "x"))

  implicit val sqsClient = createAsyncClient(sqsEndpoint, credentialsProvider)

  def randomQueueUrl(): String = sqsClient.createQueue(s"queue-${Random.nextInt}").getQueueUrl

  override protected def afterAll(): Unit = {
    super.afterAll()
    sqsServer.stopAndWait()
    Await.ready(system.terminate(), 5.seconds)
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
