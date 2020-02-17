/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.eventbridge

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

  def snsEndpoint: String = s"http://localhost:4100"

  implicit var eventBridgeClient: EventBridgeAsyncClient = _
  var eventBusName: String = _
  val source: String = "source"
  val detailType: String = "detail-type"

  private val eventBusNumber = new AtomicInteger()

  def createEventBus(): String = {
    val eventBusName = s"alpakka-event-bus-${eventBusNumber.incrementAndGet()}"
    eventBridgeClient
      .createEventBus(CreateEventBusRequest.builder().name(eventBusName).build())
      .get()
    eventBusName
  }

  override protected def beforeAll(): Unit = {
    eventBridgeClient = createAsyncClient(snsEndpoint)
    eventBusName = createEventBus()
  }

  override protected def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  def createAsyncClient(endEndpoint: String): EventBridgeAsyncClient = {
    //#init-client
    import java.net.URI

    import com.github.matsluni.akkahttpspi.AkkaHttpClient
    import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
    import software.amazon.awssdk.regions.Region

    // Don't encode credentials in your source code!
    // see https://doc.akka.io/docs/alpakka/current/aws-shared-configuration.html
    val credentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create("x", "x"))
    implicit val awsEventBridgeClient: EventBridgeAsyncClient =
      EventBridgeAsyncClient
        .builder()
        .credentialsProvider(credentialsProvider)
        //#init-client
        .endpointOverride(URI.create(endEndpoint))
        //#init-client
        .region(Region.EU_CENTRAL_1)
        .httpClient(AkkaHttpClient.builder().withActorSystem(system).build())
        // Possibility to configure the retry policy
        // see https://doc.akka.io/docs/alpakka/current/aws-shared-configuration.html
        // .overrideConfiguration(...)
        .build()

    system.registerOnTermination(awsEventBridgeClient.close())
    //#init-client
    awsEventBridgeClient
  }

  def sleep(d: FiniteDuration): Unit = Thread.sleep(d.toMillis)

}
