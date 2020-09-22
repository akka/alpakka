/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.streaming.impl

import java.util.concurrent.atomic.AtomicLong

import akka.actor.ActorSystem
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.stream.alpakka.mqtt.streaming._
import akka.stream.alpakka.mqtt.streaming.scaladsl._
import akka.stream.scaladsl.{BroadcastHub, Flow, Keep, RestartFlow, Source, SourceQueueWithComplete}
import akka.stream.{Materializer, OverflowStrategy}
import akka.util.ByteString
import akka.{Done, NotUsed}

import scala.collection.immutable
import scala.concurrent.{Future, Promise}

/**
 * Internal API
 */
@InternalApi
private[streaming] object HighLevelMqttSource {

  def atMostOnce(
      sessionSettings: MqttSessionSettings,
      transportSettings: MqttTransportSettings,
      restartSettings: MqttRestartSettings,
      connectionSettings: MqttConnectionSettings,
      subscriptions: MqttSubscribe
  ): Source[Publish, Future[immutable.Seq[(String, ControlPacketFlags)]]] = {
    type Out = Publish

    val sendAcknowledge: SourceQueueWithComplete[Command[Nothing]] => PartialFunction[Event[Nothing], Out] =
      commands => {
        // https://docs.oasis-open.org/mqtt/mqtt/v5.0/cos02/mqtt-v5.0-cos02.html#_Toc1477420
        case Event(publish @ Publish(_, _, Some(packetId), _), _) =>
          commands.offer(Command(PubAck(packetId)))
          publish
        case Event(publish: Publish, _) =>
          publish
      }

    createOnMaterialization[Out](sessionSettings,
                                 transportSettings,
                                 restartSettings,
                                 connectionSettings,
                                 subscriptions,
                                 sendAcknowledge)
  }

  def atLeastOnce[Out](
      sessionSettings: MqttSessionSettings,
      transportSettings: MqttTransportSettings,
      restartSettings: MqttRestartSettings,
      connectionSettings: MqttConnectionSettings,
      subscriptions: MqttSubscribe,
      createOut: (Publish, () => Future[Done]) => Out
  ): Source[Out, Future[immutable.Seq[(String, ControlPacketFlags)]]] = {
    val createAckHandle: SourceQueueWithComplete[Command[Nothing]] => PartialFunction[Event[Nothing], Out] =
      commands => {
        // https://docs.oasis-open.org/mqtt/mqtt/v5.0/cos02/mqtt-v5.0-cos02.html#_Toc1477420
        case Event(publish @ Publish(_, _, Some(packetId), _), _) =>
          createOut(publish,
                    () =>
                      commands
                        .offer(Command(PubAck(packetId)))
                        .map(_ => Done)(ExecutionContexts.parasitic))
        case Event(publish: Publish, _) =>
          throw new RuntimeException(s"Received Publish without packetId in at-least-once mode: $publish")
      }

    createOnMaterialization[Out](sessionSettings,
                                 transportSettings,
                                 restartSettings,
                                 connectionSettings,
                                 subscriptions,
                                 createAckHandle)
  }

  private def createOnMaterialization[Out](
      sessionSettings: MqttSessionSettings,
      transportSettings: MqttTransportSettings,
      restartSettings: MqttRestartSettings,
      connectionSettings: MqttConnectionSettings,
      subscriptions: MqttSubscribe,
      createAckHandle: SourceQueueWithComplete[
        Command[Nothing]
      ] => PartialFunction[Event[Nothing], Out]
  ) =
    Source
      .fromMaterializer { (mat, _) =>
        implicit val system: ActorSystem = mat.system

        val mqttClientSession: MqttClientSession = ActorMqttClientSession(sessionSettings)
        constructInternals[Out](mqttClientSession,
                                transportSettings,
                                restartSettings,
                                connectionSettings,
                                subscriptions,
                                createAckHandle)
          .watchTermination() {
            case (materialized, termination) =>
              termination.foreach(_ => mqttClientSession.shutdown())(system.dispatcher)
              materialized
          }
      }
      .mapMaterializedValue(_.flatMap(identity)(ExecutionContexts.parasitic))

  private def constructInternals[Out](
      mqttClientSession: MqttClientSession,
      transport: MqttTransportSettings,
      restartSettings: MqttRestartSettings,
      connect: MqttConnectPacket,
      subscribe: MqttSubscribe,
      acknowledgeAndOut: SourceQueueWithComplete[Command[Nothing]] => PartialFunction[Event[Nothing], Out]
  )(implicit system: ActorSystem, materializer: Materializer) = {
    val initCommands = immutable.Seq(
      Command(connect.controlPacket),
      Command(subscribe.controlPacket)
    )

    val connectionIdFunction: () => ByteString = {
      val counter = new AtomicLong()
      () => ByteString(counter.incrementAndGet().toString)
    }

    val mqttFlow: Flow[Command[Nothing], Either[MqttCodec.DecodeError, Event[Nothing]], NotUsed] = {
      import restartSettings._
      RestartFlow.onFailuresWithBackoff(minBackoff, maxBackoff, randomFactor, maxRestarts) { () =>
        Flow[Command[Nothing]]
          .prepend(Source(initCommands))
          .via(
            Mqtt
              .clientSessionFlow(mqttClientSession, connectionIdFunction())
              .join(transport.connectionFlow())
          )
      }
    }
    val subscribed = Promise[immutable.Seq[(String, ControlPacketFlags)]]()

    val (commands: SourceQueueWithComplete[Command[Nothing]], subscription: Source[Event[Nothing], NotUsed]) =
      Source
        .queue[Command[Nothing]](connect.bufferSize, OverflowStrategy.backpressure)
        .via(mqttFlow)
        .map {
          case Left(decodeError) =>
            throw new RuntimeException(decodeError.toString)
          case Right(event @ Event(s: SubAck, _)) =>
            val subscriptionAnswer = subscribe.controlPacket.topicFilters.map(_._1).zip(s.returnCodes)
            subscribed.trySuccess(subscriptionAnswer.toIndexedSeq)
            event
          // TODO https://github.com/akka/alpakka/pull/1565#discussion_r267089165
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
          case (publishSourceCompletion, streamTermination) =>
            // shut down the client flow
            streamTermination
              .flatMap(_ => commands.offer(Command(Disconnect)))(system.dispatcher)
              .foreach { _ =>
                commands.complete()
              }(system.dispatcher)
            publishSourceCompletion
        }
    publishSource
  }
}
