/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.mqtt.streaming
package impl

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.typed.scaladsl.Behaviors
import akka.stream.{Materializer, OverflowStrategy, SystemMaterializer}
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import akka.util.ByteString
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.Promise
import scala.concurrent.duration._

class ConsumerShutdownSpec
    extends AnyWordSpec
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures
    with LogCapturing {
  private val testKit = ActorTestKit()
  private implicit val mat: Materializer = SystemMaterializer(testKit.system).materializer
  private implicit val patience: PatienceConfig = PatienceConfig(3.seconds, 10.millis)

  override def afterAll(): Unit = testKit.shutdownTestKit()

  private val packetId = PacketId(1)
  private val publication = Publish("topic", ByteString("payload")).copy(
    flags = ControlPacketFlags(PublishQoSFlags.QoSAtLeastOnceDelivery.underlying | ControlPacketFlags.DUP.underlying),
    packetId = Some(packetId)
  )

  "duplicate publication routing" should {
    for (server <- List(false, true)) {
      s"recover when a ${if (server) "server" else "client"} consumer stops before replying" in {
        val consumer = testKit.createTestProbe[Consumer.Event]()
        val router = testKit.createTestProbe[RemotePacketRouter.Request[Consumer.Event]]()
        val forwarded = Promise[Consumer.ForwardPublish.type]()
        val settings = MqttSessionSettings()

        val (stopConnector, completeQueue) = if (server) {
          val remote =
            Source.queue[ClientConnection.ForwardConnAckCommand](1, OverflowStrategy.fail).to(Sink.ignore).run()
          val connector = testKit.spawn(Behaviors.setup[ClientConnection.Event] { context =>
            context.watch(consumer.ref)
            ClientConnection.clientConnected(
              ClientConnection.ConnAckReplied(
                connect = Connect("client", ConnectFlags.None),
                remote = remote,
                stash = Vector.empty,
                publishers = Set.empty,
                activeConsumers = Map(publication.topicName -> consumer.ref),
                activeProducers = Map.empty,
                pendingLocalPublications = Vector.empty,
                pendingRemotePublications = Vector.empty,
                consumerPacketRouter = router.ref,
                producerPacketRouter = testKit.createTestProbe[LocalPacketRouter.Request[Producer.Event]]().ref,
                publisherPacketRouter = testKit.createTestProbe[RemotePacketRouter.Request[Publisher.Event]]().ref,
                unpublisherPacketRouter = testKit.createTestProbe[RemotePacketRouter.Request[Unpublisher.Event]]().ref,
                settings = settings
              )
            )
          })
          connector ! ClientConnection.PublishReceivedFromRemote(publication, forwarded)
          (() => testKit.stop(connector), () => remote.complete())
        } else {
          val remote =
            Source.queue[ClientConnector.ForwardConnectCommand](1, OverflowStrategy.fail).to(Sink.ignore).run()
          val connector = testKit.spawn(Behaviors.setup[ClientConnector.Event] { context =>
            context.watch(consumer.ref)
            ClientConnector.serverConnected(
              ClientConnector.ConnAckReceived(
                connectionId = ByteString("connection"),
                connectFlags = ConnectFlags.None,
                keepAlive = Duration.Zero,
                pendingPingResp = false,
                remote = remote,
                stash = Vector.empty,
                activeConsumers = Map(publication.topicName -> consumer.ref),
                activeProducers = Map.empty,
                pendingLocalPublications = Vector.empty,
                pendingRemotePublications = Vector.empty,
                consumerPacketRouter = router.ref,
                producerPacketRouter = testKit.createTestProbe[LocalPacketRouter.Request[Producer.Event]]().ref,
                subscriberPacketRouter = testKit.createTestProbe[LocalPacketRouter.Request[Subscriber.Event]]().ref,
                unsubscriberPacketRouter = testKit.createTestProbe[LocalPacketRouter.Request[Unsubscriber.Event]]().ref,
                settings = settings
              )
            )
          })
          connector ! ClientConnector.PublishReceivedFromRemote(ByteString("connection"), publication, forwarded)
          (() => testKit.stop(connector), () => remote.complete())
        }

        try {
          val route = router.expectMessageType[RemotePacketRouter.Route[Consumer.Event]]
          route.packetId shouldBe packetId
          route.clientId shouldBe (if (server) Some("client") else None)
          route.event shouldBe a[Consumer.DupPublishReceivedFromRemote]
          forwarded.isCompleted shouldBe false

          consumer.stop()
          route.failureReply.failure(RemotePacketRouter.CannotRoute(packetId))

          val registration = router.expectMessageType[RemotePacketRouter.Register[Consumer.Event]]
          registration.packetId shouldBe packetId
          registration.reply.success(RemotePacketRouter.Registered)
          forwarded.future.futureValue shouldBe Consumer.ForwardPublish

          val acknowledged = Promise[Consumer.ForwardPubAck.type]()
          registration.registrant ! Consumer.PubAckReceivedLocally(acknowledged)
          acknowledged.future.futureValue shouldBe Consumer.ForwardPubAck
        } finally {
          stopConnector()
          completeQueue()
        }
      }
    }
  }
}
