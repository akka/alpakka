/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.streaming.scaladsl

import akka.actor.ActorSystem
import akka.dispatch.ExecutionContexts
import akka.{Done, NotUsed}
import akka.stream.{Materializer, OverflowStrategy}
import akka.stream.alpakka.mqtt.streaming.impl.Setup
import akka.stream.alpakka.mqtt.streaming._
import akka.stream.scaladsl.{BroadcastHub, Flow, Keep, RestartFlow, Source, SourceQueueWithComplete, Tcp}
import akka.util.ByteString

import scala.collection.immutable
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration._

trait MqttTransportSettings {
  def connectionFlow()(implicit system: ActorSystem): Flow[ByteString, ByteString, _]
}

final class MqttTcpTransportSettings(
    val host: String = "localhost",
    val port: Int = 1883
) extends MqttTransportSettings {
  def connectionFlow()(implicit system: ActorSystem): Flow[ByteString, ByteString, _] =
    Tcp(system).outgoingConnection(host, port)
}

final class MqttConnectionSettings(
    val clientId: String,
    val connectFlags: ConnectFlags = ConnectFlags.CleanSession
) {
  def createControlPacket: Connect = Connect(clientId, connectFlags)
}

final class MqttRestartSettings(
    val minBackoff: FiniteDuration = 2.seconds,
    val maxBackoff: FiniteDuration = 2.minutes,
    val randomFactor: Double = 0.42d,
    val maxRestarts: Int = -1
)

object MqttSource {

  def atMostOnce(
      mqttClientSession: MqttClientSession,
      transportSettings: MqttTransportSettings,
      restartSettings: MqttRestartSettings,
      connectionSettings: MqttConnectionSettings,
      subscriptions: immutable.Seq[(String, ControlPacketFlags)]
  ): Source[Publish, Future[immutable.Seq[(String, ControlPacketFlags)]]] =
    Setup
      .source { implicit materializer => implicit attributes =>
        implicit val system: ActorSystem = materializer.system

        type Out = Publish

        val sendAcknowledge: SourceQueueWithComplete[Command[Nothing]] => PartialFunction[Event[Nothing], Out] =
          commands => {
            case Event(publish @ Publish(_, _, Some(packetId), _), _) =>
              commands.offer(Command(PubAck(packetId)))
              publish
            case Event(publish: Publish, _) =>
              publish
          }

        val (_, _, publishSource) =
          constructInternals(mqttClientSession,
                             transportSettings,
                             restartSettings,
                             connectionSettings,
                             subscriptions,
                             sendAcknowledge)
        publishSource
      }
      .mapMaterializedValue(_.flatten)

  final class MqttAckHandle(sendAck: => Future[Done]) {

    def ack(): Future[Done] = sendAck

  }

  def atLeastOnce(
      mqttClientSession: MqttClientSession,
      transportSettings: MqttTransportSettings,
      restartSettings: MqttRestartSettings,
      connectionSettings: MqttConnectionSettings,
      subscriptions: immutable.Seq[(String, ControlPacketFlags)],
  ): Source[(Publish, MqttAckHandle), Future[immutable.Seq[(String, ControlPacketFlags)]]] =
    Setup
      .source { implicit materializer => implicit attributes =>
        implicit val system: ActorSystem = materializer.system

        type Out = (Publish, MqttAckHandle)

        val createAckHandle: SourceQueueWithComplete[Command[Nothing]] => PartialFunction[Event[Nothing], Out] =
          commands => {
            case Event(publish @ Publish(_, _, Some(packetId), _), _) =>
              (publish,
               new MqttAckHandle(
                 commands
                   .offer(Command(PubAck(packetId)))
                   .map(_ => Done)(ExecutionContexts.sameThreadExecutionContext)
               ))
            case Event(publish: Publish, _) =>
              throw new RuntimeException("Received Publish without packetId in at-least-once mode")
          }

        val (_, _, publishSource) =
          constructInternals(mqttClientSession,
                             transportSettings,
                             restartSettings,
                             connectionSettings,
                             subscriptions,
                             createAckHandle)
        publishSource
      }
      .mapMaterializedValue(_.flatten)

  def constructInternals[Out](
      mqttClientSession: MqttClientSession,
      transportSettings: MqttTransportSettings,
      restartSettings: MqttRestartSettings,
      connectionSettings: MqttConnectionSettings,
      subscriptions: immutable.Seq[(String, ControlPacketFlags)],
      acknowledge: SourceQueueWithComplete[Command[Nothing]] => PartialFunction[Event[Nothing], Out]
  )(implicit system: ActorSystem, materializer: Materializer) = {
    val mqttFlow: Flow[Command[Nothing], Either[MqttCodec.DecodeError, Event[Nothing]], NotUsed] = {
      import restartSettings._
      RestartFlow.onFailuresWithBackoff(minBackoff, maxBackoff, randomFactor, maxRestarts) { () =>
        Mqtt
          .clientSessionFlow(mqttClientSession)
          .join(transportSettings.connectionFlow())
      }
    }
    val subscribed = Promise[immutable.Seq[(String, ControlPacketFlags)]]()

    val initCommands = immutable.Seq(
      Command(Connect(connectionSettings.clientId, connectionSettings.connectFlags)),
      Command(Subscribe(subscriptions))
    )

    val (commands: SourceQueueWithComplete[Command[Nothing]], subscription: Source[Event[Nothing], NotUsed]) =
      Source
        .queue[Command[Nothing]](10, OverflowStrategy.fail)
        .prepend(Source(initCommands))
        .via(mqttFlow)
        .map {
          case Left(decodeError) =>
            throw new RuntimeException(decodeError.toString)
          case Right(event @ Event(s: SubAck, _)) =>
            val subscriptionAnswer = subscriptions.map(_._1).zip(s.returnCodes)
            subscribed.trySuccess(subscriptionAnswer)
            event
          case Right(event) =>
            event
        }
        .toMat(BroadcastHub.sink)(Keep.both)
        .run()

    val publishSource: Source[Out, Future[immutable.Seq[(String, ControlPacketFlags)]]] =
      subscription
        .collect { acknowledge(commands) }
        .mapMaterializedValue(_ => subscribed.future)
        .watchTermination() {
          case (publishSourceCompletion, completion) =>
            completion.foreach { _ =>
              // shut down the client flow
              commands.complete()
            }(system.dispatcher)
            publishSourceCompletion
        }
    (commands, subscription, publishSource)
  }
}
