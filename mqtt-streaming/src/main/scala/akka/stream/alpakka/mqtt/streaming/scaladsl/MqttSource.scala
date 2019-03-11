/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.streaming.scaladsl

import akka.actor.ActorSystem
import akka.{Done, NotUsed}
import akka.event.{Logging, LoggingAdapter}
import akka.stream.OverflowStrategy
import akka.stream.alpakka.mqtt.streaming.impl.Setup
import akka.stream.alpakka.mqtt.streaming._
import akka.stream.scaladsl.{BroadcastHub, Flow, Keep, RestartFlow, Source, SourceQueueWithComplete, Tcp}

import scala.collection.immutable
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration._

final class MqttConnectionSettings(
    val host: String = "localhost",
    val port: Int = 1883
)

final class MqttRestartSettings(
    val minBackoff: FiniteDuration = 2.seconds,
    val maxBackoff: FiniteDuration = 2.minutes,
    val randomFactor: Double = 0.42d,
    val maxRestarts: Int = -1
)

object MqttSource {

  def atMostOnce(mqttClientSession: MqttClientSession,
                 connectionSettings: MqttConnectionSettings,
                 restartSettings: MqttRestartSettings,
                 clientId: String,
                 subscriptions: immutable.Seq[(String, ControlPacketFlags)]): Source[Publish, Future[Done]] =
    Setup
      .source { implicit materializer => implicit attributes =>
        implicit val system: ActorSystem = materializer.system
        implicit val logging: LoggingAdapter = Logging.getLogger(system, this)

        val mqttFlow: Flow[Command[Nothing], Either[MqttCodec.DecodeError, Event[Nothing]], NotUsed] = {
          import restartSettings._
          RestartFlow.onFailuresWithBackoff(minBackoff, maxBackoff, randomFactor, maxRestarts) { () =>
            Mqtt
              .clientSessionFlow(mqttClientSession)
              .join(Tcp(system).outgoingConnection(connectionSettings.host, connectionSettings.port))
          }
        }
        val subscribed = Promise[Done]()

        val initCommands = immutable.Seq(
          Command(Connect(clientId, ConnectFlags.CleanSession)),
          Command(Subscribe(subscriptions))
        )

        val (commands: SourceQueueWithComplete[Command[Nothing]], subscription: Source[Event[Nothing], NotUsed]) =
          Source
            .queue[Command[Nothing]](10, OverflowStrategy.fail)
            .prepend(Source(initCommands))
            .log("sending")
            .via(mqttFlow)
            .log("source received")
            .map {
              case Left(decodeError) =>
                throw new RuntimeException(decodeError.toString)
              case Right(event @ Event(_: SubAck, _)) =>
                subscribed.trySuccess(Done)
                event
              case Right(event) =>
                event
            }
            .toMat(BroadcastHub.sink)(Keep.both)
            .run()

        val publishSource: Source[Publish, Future[Done]] = subscription
          .log("publishSource")
          .collect {
            case Event(publish @ Publish(_, _, Some(packetId), _), _) =>
              commands.offer(Command(PubAck(packetId)))
              publish
            case Event(publish: Publish, _) =>
              publish
          }
          .mapMaterializedValue(_ => subscribed.future)
          .watchTermination() {
            case (publishSourceCompletion, completion) =>
              completion.foreach { _ =>
                // shut down the client flow
                commands.complete()
              }(system.dispatcher)
              publishSourceCompletion
          }
        publishSource
      }
      .mapMaterializedValue(_.flatten)
}
