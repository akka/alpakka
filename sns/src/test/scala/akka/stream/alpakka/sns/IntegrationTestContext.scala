/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sns

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.TestKit
import com.amazonaws.services.sns.{AmazonSNSAsync, AmazonSNSAsyncClientBuilder}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

import scala.concurrent.duration.FiniteDuration
import scala.util.Random

trait IntegrationTestContext extends BeforeAndAfterAll with ScalaFutures { this: Suite =>

  //#init-system
  implicit val system: ActorSystem = ActorSystem()
  implicit val mat: Materializer = ActorMaterializer()
  //#init-system

  def snsEndpoint: String = s"http://localhost:4100"

  implicit var snsClient: AmazonSNSAsync = _
  var topicArn: String = _

  private val topicNumber = new AtomicInteger()

  def createTopic(): String = snsClient.createTopic(s"alpakka-topic-${topicNumber.incrementAndGet()}").getTopicArn

  override protected def beforeAll(): Unit = {
    snsClient = createAsyncClient(snsEndpoint)
    topicArn = createTopic()
  }

  override protected def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  def createAsyncClient(endEndpoint: String): AmazonSNSAsync = {
    //#init-client
    import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
    import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration

    val credentialsProvider = new AWSStaticCredentialsProvider(new BasicAWSCredentials("x", "x"))
    implicit val awsSnsClient: AmazonSNSAsync = AmazonSNSAsyncClientBuilder
      .standard()
      .withCredentials(credentialsProvider)
      .withEndpointConfiguration(new EndpointConfiguration(endEndpoint, "eu-central-1"))
      .build()
    system.registerOnTermination(awsSnsClient.shutdown())
    //#init-client
    awsSnsClient
  }

  def sleep(d: FiniteDuration): Unit = Thread.sleep(d.toMillis)

}
