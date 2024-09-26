/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.sns

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Suite}
import software.amazon.awssdk.services.sns.SnsAsyncClient
import software.amazon.awssdk.services.sns.model.CreateTopicRequest

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

trait IntegrationTestContext extends BeforeAndAfterAll with ScalaFutures {
  this: Suite =>

  //#init-system
  implicit val system: ActorSystem = ActorSystem()
  //#init-system

  def snsEndpoint: String = s"http://localhost:4100"

  implicit var snsClient: SnsAsyncClient = _
  var topicArn: String = _

  private val topicNumber = new AtomicInteger()

  def createTopic(): String =
    snsClient
      .createTopic(CreateTopicRequest.builder().name(s"alpakka-topic-${topicNumber.incrementAndGet()}").build())
      .get()
      .topicArn()

  override protected def beforeAll(): Unit = {
    snsClient = createAsyncClient(snsEndpoint)
    topicArn = createTopic()
  }

  override protected def afterAll(): Unit = {
    Http()
      .shutdownAllConnectionPools()
      .flatMap(_ => system.terminate())(ExecutionContext.global)
      .futureValue
  }

  def createAsyncClient(endEndpoint: String): SnsAsyncClient = {
    //#init-client
    import java.net.URI

    import com.github.matsluni.akkahttpspi.AkkaHttpClient
    import software.amazon.awssdk.services.sns.SnsAsyncClient
    import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
    import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
    import software.amazon.awssdk.regions.Region

    // Don't encode credentials in your source code!
    // see https://doc.akka.io/libraries/alpakka/current/aws-shared-configuration.html
    val credentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create("x", "x"))
    implicit val awsSnsClient: SnsAsyncClient =
      SnsAsyncClient
        .builder()
        .credentialsProvider(credentialsProvider)
        //#init-client
        .endpointOverride(URI.create(endEndpoint))
        //#init-client
        .region(Region.EU_CENTRAL_1)
        .httpClient(AkkaHttpClient.builder().withActorSystem(system).build())
        // Possibility to configure the retry policy
        // see https://doc.akka.io/libraries/alpakka/current/aws-shared-configuration.html
        // .overrideConfiguration(...)
        .build()

    system.registerOnTermination(awsSnsClient.close())
    //#init-client
    awsSnsClient
  }

  def sleep(d: FiniteDuration): Unit = Thread.sleep(d.toMillis)

}
