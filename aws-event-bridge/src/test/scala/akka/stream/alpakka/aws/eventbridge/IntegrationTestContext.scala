/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.aws.eventbridge

import java.util.UUID

import akka.actor.ActorSystem
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
  //#init-system

  def eventBusEndpoint: String = s"http://localhost:4587"

  implicit var eventBridgeClient: EventBridgeAsyncClient = _
  var eventBusArn: String = _

  def createEventBus(): String =
    eventBridgeClient
      .createEventBus(
        CreateEventBusRequest.builder().name(s"alpakka-topic-${UUID.randomUUID().toString}").build()
      )
      .get()
      .eventBusArn()

  override protected def beforeAll(): Unit = {
    eventBridgeClient = createAsyncClient(eventBusEndpoint)
    eventBusArn = createEventBus()
  }

  override protected def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  def createAsyncClient(endEndpoint: String): EventBridgeAsyncClient = {
    //#init-client
    import java.net.URI

    import com.github.matsluni.akkahttpspi.AkkaHttpClient
    import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
    import software.amazon.awssdk.regions.Region
    import software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient

    implicit val awsEventBridgeClient: EventBridgeAsyncClient =
      EventBridgeAsyncClient
        .builder()
        .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("x", "x")))
        .endpointOverride(URI.create(endEndpoint))
        .region(Region.EU_CENTRAL_1)
        .httpClient(AkkaHttpClient.builder().withActorSystem(system).build())
        .build()

    system.registerOnTermination(awsEventBridgeClient.close())
    //#init-client
    awsEventBridgeClient
  }

  def sleep(d: FiniteDuration): Unit = Thread.sleep(d.toMillis)

}
