/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.sqs.scaladsl

import java.net.URI
import java.util.concurrent.TimeUnit

import akka.actor.{ActorSystem, Terminated}
import akka.http.scaladsl.Http
import akka.stream.alpakka.sqs.SqsSourceSettings
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, Suite, Tag}

import scala.concurrent.ExecutionContext
//#init-client
import com.github.matsluni.akkahttpspi.AkkaHttpClient
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest
//#init-client
import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.util.Random

trait DefaultTestContext extends Matchers with BeforeAndAfterAll with ScalaFutures { this: Suite =>

  //#init-mat
  implicit val system: ActorSystem = ActorSystem()
  //#init-mat

  // endpoint of the elasticmq docker container
  val sqsEndpoint: String = "http://localhost:9324"

  object Integration extends Tag("akka.stream.alpakka.sqs.scaladsl.Integration")

  implicit val pc: PatienceConfig = PatienceConfig(10.seconds, 20.millis)

  lazy val sqsClient = createAsyncClient(sqsEndpoint)

  //ElasticMQ has a bug: when you set wait time seconds > 0,
  //sometimes the server does not return any message and blocks the 20 seconds, even if a message arrives later.
  //this helps the tests to become a little less intermittent. =)
  val sqsSourceSettings = SqsSourceSettings.Defaults.withWaitTimeSeconds(0)

  def randomQueueUrl(): String =
    sqsClient
      .createQueue(CreateQueueRequest.builder().queueName(s"queue-${Random.nextInt()}").build())
      .get(2, TimeUnit.SECONDS)
      .queueUrl()

  val fifoQueueRequest =
    CreateQueueRequest
      .builder()
      .queueName(s"queue-${Random.nextInt()}.fifo")
      .attributesWithStrings(Map("FifoQueue" -> "true", "ContentBasedDeduplication" -> "true").asJava)
      .build()

  def randomFifoQueueUrl(): String = sqsClient.createQueue(fifoQueueRequest).get(2, TimeUnit.SECONDS).queueUrl()

  override protected def afterAll(): Unit =
    try {
      closeSqsClient()
      Http()
        .shutdownAllConnectionPools()
        .flatMap(_ => system.terminate())(ExecutionContext.global)
        .futureValue shouldBe a[Terminated]
    } finally {
      super.afterAll()
    }

  protected def closeSqsClient(): Unit = sqsClient.close()

  def createAsyncClient(sqsEndpoint: String): SqsAsyncClient = {
    //#init-client

    // Don't encode credentials in your source code!
    // see https://doc.akka.io/docs/alpakka/current/aws-shared-configuration.html
    val credentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create("x", "x"))
    implicit val awsSqsClient = SqsAsyncClient
      .builder()
      .credentialsProvider(credentialsProvider)
      // #init-client
      .endpointOverride(URI.create(sqsEndpoint))
      // #init-client
      .region(Region.EU_CENTRAL_1)
      .httpClient(AkkaHttpClient.builder().withActorSystem(system).build())
      // Possibility to configure the retry policy
      // see https://doc.akka.io/docs/alpakka/current/aws-shared-configuration.html
      // .overrideConfiguration(...)
      .build()

    system.registerOnTermination(awsSqsClient.close())
    //#init-client
    awsSqsClient
  }
}
