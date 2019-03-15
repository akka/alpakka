/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.streaming.impl

import akka.actor.ActorSystem
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.stream.alpakka.mqtt.streaming._
import akka.stream.alpakka.mqtt.streaming.scaladsl._
import akka.stream.scaladsl.{BroadcastHub, Flow, Keep, RestartFlow, Source, SourceQueueWithComplete}
import akka.stream.{Materializer, OverflowStrategy}
import akka.{Done, NotUsed}

import scala.collection.immutable
import scala.concurrent.{Future, Promise}

@InternalApi
private[streaming] object HighLevelMqttSource {

  def atMostOnce(
      mqttClientSession: MqttClientSession,
      transportSettings: MqttTransportSettings,
      restartSettings: MqttRestartSettings,
      connectionSettings: MqttConnectionSettings,
      subscriptions: MqttSubscribe
  ): Source[Publish, Future[immutable.Seq[(String, ControlPacketFlags)]]] = {
    type Out = Publish

    val sendAcknowledge: SourceQueueWithComplete[Command[Nothing]] => PartialFunction[Event[Nothing], Out] =
      commands => {
        case Event(publish @ Publish(_, _, Some(packetId), _), _) =>
          commands.offer(Command(PubAck(packetId)))
          publish
        case Event(publish: Publish, _) =>
          publish
      }

    apply(mqttClientSession,
          transportSettings,
          restartSettings,
          connectionSettings,
          subscriptions,
          sendAcknowledge,
          bufferSize = 10)
  }

  def atLeastOnce[Out](
      mqttClientSession: MqttClientSession,
      transportSettings: MqttTransportSettings,
      restartSettings: MqttRestartSettings,
      connectionSettings: MqttConnectionSettings,
      subscriptions: MqttSubscribe,
      createOut: (Publish, () => Future[Done]) => Out
  ): Source[Out, Future[immutable.Seq[(String, ControlPacketFlags)]]] = {
    val createAckHandle: SourceQueueWithComplete[Command[Nothing]] => PartialFunction[Event[Nothing], Out] =
      commands => {
        case Event(publish @ Publish(_, _, Some(packetId), _), _) =>
          createOut(publish,
                    () =>
                      commands
                        .offer(Command(PubAck(packetId)))
                        .map(_ => Done)(ExecutionContexts.sameThreadExecutionContext))
        case Event(publish: Publish, _) =>
          throw new RuntimeException(s"Received Publish without packetId in at-least-once mode: $publish")
      }

    apply(mqttClientSession,
          transportSettings,
          restartSettings,
          connectionSettings,
          subscriptions,
          createAckHandle,
          bufferSize = 10)
  }

  private def apply[Out](
      mqttClientSession: MqttClientSession,
      transportSettings: MqttTransportSettings,
      restartSettings: MqttRestartSettings,
      connectionSettings: MqttConnectionSettings,
      subscriptions: MqttSubscribe,
      createAckHandle: SourceQueueWithComplete[
        Command[Nothing]
      ] => PartialFunction[Event[Nothing], Out],
      bufferSize: Int
  ) =
    Setup
      .source { implicit materializer => implicit attributes =>
        implicit val system: ActorSystem = materializer.system

        constructInternals[Out](mqttClientSession,
                                transportSettings,
                                restartSettings,
                                connectionSettings,
                                bufferSize = 10,
                                subscriptions,
                                createAckHandle)
      }
      .mapMaterializedValue(_.flatten)

  private def constructInternals[Out](
      mqttClientSession: MqttClientSession,
      transport: MqttTransportSettings,
      restartSettings: MqttRestartSettings,
      connect: MqttConnectPacket,
      bufferSize: Int,
      subscribe: MqttSubscribe,
      acknowledgeAndOut: SourceQueueWithComplete[Command[Nothing]] => PartialFunction[Event[Nothing], Out]
  )(implicit system: ActorSystem, materializer: Materializer) = {
    val mqttFlow: Flow[Command[Nothing], Either[MqttCodec.DecodeError, Event[Nothing]], NotUsed] = {
      import restartSettings._
      RestartFlow.onFailuresWithBackoff(minBackoff, maxBackoff, randomFactor, maxRestarts) { () =>
        Mqtt
          .clientSessionFlow(mqttClientSession)
          .join(transport.connectionFlow())
      }
    }
    val subscribed = Promise[immutable.Seq[(String, ControlPacketFlags)]]()

    val subsribePacket = subscribe.controlPacket
    val initCommands = immutable.Seq(
      Command(connect.controlPacket),
      Command(subsribePacket)
    )

    val (commands: SourceQueueWithComplete[Command[Nothing]], subscription: Source[Event[Nothing], NotUsed]) =
      Source
        .queue[Command[Nothing]](bufferSize, OverflowStrategy.backpressure)
        .prepend(Source(initCommands))
        .via(mqttFlow)
        .map {
          case Left(decodeError) =>
            throw new RuntimeException(decodeError.toString)
          case Right(event @ Event(s: SubAck, _)) =>
            val subscriptionAnswer = subsribePacket.topicFilters.map(_._1).zip(s.returnCodes)
            subscribed.trySuccess(subscriptionAnswer)
            event
          case Right(event) =>
            event
        }
        .toMat(BroadcastHub.sink)(Keep.both)
        .run()

    val publishSource: Source[Out, Future[immutable.Seq[(String, ControlPacketFlags)]]] =
      subscription
        .collect { acknowledgeAndOut(commands) }
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
}
