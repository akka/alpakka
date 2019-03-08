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
import akka.stream.scaladsl.{BroadcastHub, Flow, Keep, Source, SourceQueueWithComplete, Tcp}

import scala.collection.immutable
import scala.concurrent.{Future, Promise}

object MqttSource {

  def atMostOnce(mqttClientSession: MqttClientSession,
                 clientId: String,
                 subscriptions: immutable.Seq[(String, ControlPacketFlags)]): Source[Publish, Future[Done]] =
    Setup
      .source { implicit materializer => implicit attributes =>
        implicit val system: ActorSystem = materializer.system
        implicit val logging: LoggingAdapter = Logging.getLogger(system, this)

        val mqttFlow: Flow[Command[Nothing], Either[MqttCodec.DecodeError, Event[Nothing]], NotUsed] =
          Mqtt
            .clientSessionFlow(mqttClientSession)
            .join(Tcp(system).outgoingConnection("localhost", 1883))

        val subscribed = Promise[Done]()

        val (commands: SourceQueueWithComplete[Command[Nothing]], subscription: Source[Event[Nothing], NotUsed]) =
          Source
            .queue[Command[Nothing]](10, OverflowStrategy.fail)
            .via(mqttFlow)
            .log("source received")
            .map {
              case Left(decodeError) =>
                throw new RuntimeException(decodeError.toString)
// TODO anything we should do if no ConnAck arrives "in time"?
//            case Right(event @ Event(_: ConnAck, _)) =>
//              event
              case Right(event @ Event(_: SubAck, _)) =>
                subscribed.trySuccess(Done)
                event
              case Right(event) =>
                event
            }
            .toMat(BroadcastHub.sink)(Keep.both)
            .run()

        commands.offer(Command(Connect(clientId, ConnectFlags.CleanSession)))
        commands.offer(Command(Subscribe(subscriptions)))

        val publishSource: Source[Publish, Future[Done]] = subscription
          .collect {
            case Event(publish @ Publish(flags, _, Some(packetId), _), _) =>
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
