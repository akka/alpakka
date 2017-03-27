/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.sqs.scaladsl

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.amazonaws.auth.{AWSCredentialsProvider, BasicAWSCredentials}
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

  implicit val sqsClient = SqsUtils.createAsyncClient(sqsEndpoint)

  def randomQueueUrl(): String = sqsClient.createQueue(s"queue-${Random.nextInt}").getQueueUrl

  override protected def afterAll(): Unit = {
    super.afterAll()
    sqsServer.stopAndWait()
    Await.ready(system.terminate(), 5.seconds)
  }
}

object SqsUtils {
  def createAsyncClient(sqsEndpoint: String): AmazonSQSAsync = {
    //#init-client
    val clientBuilder = AmazonSQSAsyncClientBuilder.standard()
    val credentialsProvider = new AWSCredentialsProvider {
      override def refresh(): Unit = ()
      override def getCredentials = new BasicAWSCredentials("x", "x")
    }

    clientBuilder
      .withCredentials(credentialsProvider)
      .withEndpointConfiguration(new EndpointConfiguration(sqsEndpoint, "eu-central-1"))
      .build()
    //#init-client
  }
}
