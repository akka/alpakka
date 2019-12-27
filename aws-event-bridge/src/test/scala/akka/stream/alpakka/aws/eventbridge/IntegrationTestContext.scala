/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.aws.eventbridge

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.TestKit
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Suite}
import software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient
import software.amazon.awssdk.services.eventbridge.model.CreateEventBusRequest

import scala.concurrent.duration.FiniteDuration

trait IntegrationTestContext extends BeforeAndAfterAll with ScalaFutures {
  this: Suite =>

  //#init-system
  implicit val system: ActorSystem = ActorSystem()
  implicit val mat: Materializer = ActorMaterializer()
  //#init-system

  def eventBusEndpoint: String = s"http://localhost:4587"

  implicit var eventBridgeClient: EventBridgeAsyncClient = _
  var eventBusArn: String = _

  private val eventBusNumber = new AtomicInteger()

  def createTopic(): String =
    eventBridgeClient
      .createEventBus(
        CreateEventBusRequest.builder().name(s"alpakka-topic-${eventBusNumber.incrementAndGet()}").build()
      )
      .get()
      .eventBusArn()

  override protected def beforeAll(): Unit = {
    eventBridgeClient = createAsyncClient(eventBusEndpoint)
    eventBusArn = createTopic()
  }

  override protected def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  def createAsyncClient(endEndpoint: String): EventBridgeAsyncClient = {
    //#init-client
    import java.net.URI

    import com.github.matsluni.akkahttpspi.AkkaHttpClient
    import software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient
    import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
    import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
    import software.amazon.awssdk.regions.Region

    implicit val awsSnsClient: EventBridgeAsyncClient =
      EventBridgeAsyncClient
        .builder()
        .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("x", "x")))
        .endpointOverride(URI.create(endEndpoint))
        .region(Region.EU_CENTRAL_1)
        .httpClient(AkkaHttpClient.builder().withActorSystem(system).build())
        .build()

    system.registerOnTermination(awsSnsClient.close())
    //#init-client
    awsSnsClient
  }

  def sleep(d: FiniteDuration): Unit = Thread.sleep(d.toMillis)

}
