/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.streaming
package impl

import akka.NotUsed
import akka.actor.typed._
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.annotation.InternalApi
import akka.stream.{Materializer, OverflowStrategy, QueueOfferResult}
import akka.stream.scaladsl.{BroadcastHub, Keep, Source, SourceQueueWithComplete}
import akka.util.ByteString
import scala.collection.immutable.Seq
import scala.concurrent.Promise
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NoStackTrace
import scala.util.{Either, Failure, Success}

/*
 * A client connector is a Finite State Machine that manages MQTT client
 * session state. A client connects to a server, subscribes/unsubscribes
 * from topics to receive publications on and publishes to its own topics.
 */
@InternalApi private[streaming] object ClientConnector {

  type ConnectData = Option[_]

  /*
   * No ACK received - the CONNECT failed
   */
  case object ConnectFailed extends Exception with NoStackTrace

  /*
   * A PINGREQ failed to receive a PINGRESP - the connection must close
   *
   * http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html
   * 3.1.2.10 Keep Alive
   */
  case object PingFailed extends Exception with NoStackTrace

  /*
   * Construct with the starting state
   */
  def apply(settings: MqttSessionSettings)(implicit mat: Materializer): Behavior[Event] = Behaviors.setup { context =>
    val consumerPacketRouter =
      context.spawn(RemotePacketRouter[Consumer.Event], "client-consumer-packet-id-allocator")
    val producerPacketRouter =
      context.spawn(LocalPacketRouter[Producer.Event], "client-producer-packet-id-allocator")
    val subscriberPacketRouter =
      context.spawn(LocalPacketRouter[Subscriber.Event], "client-subscriber-packet-id-allocator")
    val unsubscriberPacketRouter =
      context.spawn(LocalPacketRouter[Unsubscriber.Event], "client-unsubscriber-packet-id-allocator")

    disconnected(
      Disconnected(
        Vector.empty,
        Map.empty,
        Map.empty,
        Vector.empty,
        Vector.empty,
        consumerPacketRouter,
        producerPacketRouter,
        subscriberPacketRouter,
        unsubscriberPacketRouter,
        settings
      )
    )
  }

  // Our FSM data, FSM events and commands emitted by the FSM

  sealed abstract class Data(val stash: Seq[Event],
                             val activeConsumers: Map[String, ActorRef[Consumer.Event]],
                             val activeProducers: Map[String, ActorRef[Producer.Event]],
                             val pendingLocalPublications: Seq[(String, PublishReceivedLocally)],
                             val pendingRemotePublications: Seq[(String, PublishReceivedFromRemote)],
                             val consumerPacketRouter: ActorRef[RemotePacketRouter.Request[Consumer.Event]],
                             val producerPacketRouter: ActorRef[LocalPacketRouter.Request[Producer.Event]],
                             val subscriberPacketRouter: ActorRef[LocalPacketRouter.Request[Subscriber.Event]],
                             val unsubscriberPacketRouter: ActorRef[LocalPacketRouter.Request[Unsubscriber.Event]],
                             val settings: MqttSessionSettings)
  final case class Disconnected(
      override val stash: Seq[Event],
      override val activeConsumers: Map[String, ActorRef[Consumer.Event]],
      override val activeProducers: Map[String, ActorRef[Producer.Event]],
      override val pendingLocalPublications: Seq[(String, PublishReceivedLocally)],
      override val pendingRemotePublications: Seq[(String, PublishReceivedFromRemote)],
      override val consumerPacketRouter: ActorRef[RemotePacketRouter.Request[Consumer.Event]],
      override val producerPacketRouter: ActorRef[LocalPacketRouter.Request[Producer.Event]],
      override val subscriberPacketRouter: ActorRef[LocalPacketRouter.Request[Subscriber.Event]],
      override val unsubscriberPacketRouter: ActorRef[LocalPacketRouter.Request[Unsubscriber.Event]],
      override val settings: MqttSessionSettings
  ) extends Data(
        stash,
        activeConsumers,
        activeProducers,
        pendingLocalPublications,
        pendingRemotePublications,
        consumerPacketRouter,
        producerPacketRouter,
        subscriberPacketRouter,
        unsubscriberPacketRouter,
        settings
      )
  final case class ConnectReceived(
      connectionId: ByteString,
      connect: Connect,
      connectData: ConnectData,
      remote: SourceQueueWithComplete[ForwardConnectCommand],
      override val stash: Seq[Event],
      override val activeConsumers: Map[String, ActorRef[Consumer.Event]],
      override val activeProducers: Map[String, ActorRef[Producer.Event]],
      override val pendingLocalPublications: Seq[(String, PublishReceivedLocally)],
      override val pendingRemotePublications: Seq[(String, PublishReceivedFromRemote)],
      override val consumerPacketRouter: ActorRef[RemotePacketRouter.Request[Consumer.Event]],
      override val producerPacketRouter: ActorRef[LocalPacketRouter.Request[Producer.Event]],
      override val subscriberPacketRouter: ActorRef[LocalPacketRouter.Request[Subscriber.Event]],
      override val unsubscriberPacketRouter: ActorRef[LocalPacketRouter.Request[Unsubscriber.Event]],
      override val settings: MqttSessionSettings
  ) extends Data(
        stash,
        activeConsumers,
        activeProducers,
        pendingLocalPublications,
        pendingRemotePublications,
        consumerPacketRouter,
        producerPacketRouter,
        subscriberPacketRouter,
        unsubscriberPacketRouter,
        settings
      )
  final case class ConnAckReceived(
      connectionId: ByteString,
      connectFlags: ConnectFlags,
      keepAlive: FiniteDuration,
      pendingPingResp: Boolean,
      remote: SourceQueueWithComplete[ForwardConnectCommand],
      override val stash: Seq[Event],
      override val activeConsumers: Map[String, ActorRef[Consumer.Event]],
      override val activeProducers: Map[String, ActorRef[Producer.Event]],
      override val pendingLocalPublications: Seq[(String, PublishReceivedLocally)],
      override val pendingRemotePublications: Seq[(String, PublishReceivedFromRemote)],
      override val consumerPacketRouter: ActorRef[RemotePacketRouter.Request[Consumer.Event]],
      override val producerPacketRouter: ActorRef[LocalPacketRouter.Request[Producer.Event]],
      override val subscriberPacketRouter: ActorRef[LocalPacketRouter.Request[Subscriber.Event]],
      override val unsubscriberPacketRouter: ActorRef[LocalPacketRouter.Request[Unsubscriber.Event]],
      override val settings: MqttSessionSettings
  ) extends Data(
        stash,
        activeConsumers,
        activeProducers,
        pendingLocalPublications,
        pendingRemotePublications,
        consumerPacketRouter,
        producerPacketRouter,
        subscriberPacketRouter,
        unsubscriberPacketRouter,
        settings
      )

  final case class WaitingForQueueOfferResult(nextBehavior: Behavior[Event], stash: Seq[Event])

  sealed abstract class Event(val connectionId: ByteString)

  final case class Routers(
      consumerPacketRouter: ActorRef[RemotePacketRouter.Request[Consumer.Event]],
      producerPacketRouter: ActorRef[LocalPacketRouter.Request[Producer.Event]],
      subscriberPacketRouter: ActorRef[LocalPacketRouter.Request[Subscriber.Event]],
      unsubscriberPacketRouter: ActorRef[LocalPacketRouter.Request[Unsubscriber.Event]]
  )

  final case class RequestRouters(override val connectionId: ByteString, replyTo: ActorRef[Routers])
      extends Event(connectionId)

  final case class ConnectReceivedLocally(override val connectionId: ByteString,
                                          connect: Connect,
                                          connectData: ConnectData,
                                          remote: Promise[Source[ForwardConnectCommand, NotUsed]])
      extends Event(connectionId)
  final case class ConnAckReceivedFromRemote(override val connectionId: ByteString,
                                             connAck: ConnAck,
                                             local: Promise[ForwardConnAck])
      extends Event(connectionId)

  case class ReceiveConnAckTimeout(override val connectionId: ByteString) extends Event(connectionId)

  case class ConnectionLost(override val connectionId: ByteString) extends Event(connectionId)

  final case class DisconnectReceivedLocally(override val connectionId: ByteString,
                                             remote: Promise[ForwardDisconnect.type])
      extends Event(connectionId)

  final case class SubscribeReceivedLocally(override val connectionId: ByteString,
                                            subscribe: Subscribe,
                                            subscribeData: Subscriber.SubscribeData,
                                            remote: Promise[Subscriber.ForwardSubscribe])
      extends Event(connectionId)

  final case class PublishReceivedFromRemote(override val connectionId: ByteString,
                                             publish: Publish,
                                             local: Promise[Consumer.ForwardPublish.type])
      extends Event(connectionId)

  final case class ConsumerFree(topicName: String) extends Event(ByteString.empty)

  final case class PublishReceivedLocally(publish: Publish, publishData: Producer.PublishData)
      extends Event(ByteString.empty)

  final case class ProducerFree(topicName: String) extends Event(ByteString.empty)

  case class SendPingReqTimeout(override val connectionId: ByteString) extends Event(connectionId)

  final case class PingRespReceivedFromRemote(override val connectionId: ByteString,
                                              local: Promise[ForwardPingResp.type])
      extends Event(connectionId)

  final case class ReceivedProducerPublishingCommand(command: Producer.ForwardPublishingCommand)
      extends Event(ByteString.empty)

  final case class UnsubscribeReceivedLocally(override val connectionId: ByteString,
                                              unsubscribe: Unsubscribe,
                                              unsubscribeData: Unsubscriber.UnsubscribeData,
                                              remote: Promise[Unsubscriber.ForwardUnsubscribe])
      extends Event(connectionId)

  final case class QueueOfferCompleted(override val connectionId: ByteString,
                                       result: Either[Throwable, QueueOfferResult])
      extends Event(connectionId)
      with QueueOfferState.QueueOfferCompleted

  sealed abstract class Command
  sealed abstract class ForwardConnectCommand
  case object ForwardConnect extends ForwardConnectCommand
  case object ForwardPingReq extends ForwardConnectCommand
  final case class ForwardPublish(publish: Publish, packetId: Option[PacketId]) extends ForwardConnectCommand
  final case class ForwardPubRel(packetId: PacketId) extends ForwardConnectCommand
  final case class ForwardConnAck(connectData: ConnectData) extends Command
  case object ForwardDisconnect extends Command
  case object ForwardPingResp extends Command

  // State event handling

  private val ConsumerNamePrefix = "consumer-"
  private val ProducerNamePrefix = "producer-"

  def disconnected(data: Disconnected)(implicit mat: Materializer): Behavior[Event] =
    Behaviors
      .receivePartial[Event] {
        case (context, ConnectReceivedLocally(connectionId, connect, connectData, remote)) =>
          val (queue, source) = Source
            .queue[ForwardConnectCommand](data.settings.clientSendBufferSize, OverflowStrategy.backpressure)
            .toMat(BroadcastHub.sink)(Keep.both)
            .run()

          remote.success(source)

          val nextState =
            if (connect.connectFlags.contains(ConnectFlags.CleanSession)) {
              context.children
                .filterNot( // don't stop the routers
                  child =>
                    Set[ActorRef[_]](data.consumerPacketRouter,
                                     data.producerPacketRouter,
                                     data.subscriberPacketRouter,
                                     data.unsubscriberPacketRouter) contains child
                )
                .foreach(context.stop)

              serverConnect(
                ConnectReceived(
                  connectionId,
                  connect,
                  connectData,
                  queue,
                  Vector.empty,
                  Map.empty,
                  Map.empty,
                  Vector.empty,
                  Vector.empty,
                  data.consumerPacketRouter,
                  data.producerPacketRouter,
                  data.subscriberPacketRouter,
                  data.unsubscriberPacketRouter,
                  data.settings
                )
              )
            } else {
              data.activeProducers.values.foreach { producer =>
                producer ! Producer.ReceiveConnect
              }

              serverConnect(
                ConnectReceived(
                  connectionId,
                  connect,
                  connectData,
                  queue,
                  Vector.empty,
                  data.activeConsumers,
                  data.activeProducers,
                  data.pendingLocalPublications,
                  data.pendingRemotePublications,
                  data.consumerPacketRouter,
                  data.producerPacketRouter,
                  data.subscriberPacketRouter,
                  data.unsubscriberPacketRouter,
                  data.settings
                )
              )
            }

          QueueOfferState.waitForQueueOfferCompleted(
            queue.offer(ForwardConnect),
            result => QueueOfferCompleted(connectionId, result.toEither),
            nextState,
            data.stash
          )

        case (_, RequestRouters(_, replyTo)) =>
          replyTo ! Routers(data.consumerPacketRouter,
                            data.producerPacketRouter,
                            data.subscriberPacketRouter,
                            data.unsubscriberPacketRouter)
          Behaviors.same

        case (_, ConnectionLost(_)) =>
          Behaviors.same

        case (_, e) =>
          disconnected(data.copy(stash = data.stash :+ e))
      }
      .receiveSignal {
        case (_, _: Terminated) =>
          Behaviors.same
      }

  def disconnect(context: ActorContext[Event], remote: SourceQueueWithComplete[ForwardConnectCommand], data: Data)(
      implicit mat: Materializer
  ): Behavior[Event] = {
    remote.complete()

    BehaviorRunner.run(
      disconnected(
        Disconnected(
          Vector.empty,
          data.activeConsumers,
          data.activeProducers,
          data.pendingLocalPublications,
          data.pendingRemotePublications,
          data.consumerPacketRouter,
          data.producerPacketRouter,
          data.subscriberPacketRouter,
          data.unsubscriberPacketRouter,
          data.settings
        )
      ),
      context,
      data.stash.map(BehaviorRunner.StoredMessage.apply)
    )
  }

  def serverConnect(data: ConnectReceived)(implicit mat: Materializer): Behavior[Event] = Behaviors.withTimers {
    val ReceiveConnAck = "receive-connack"

    timer =>
      if (!timer.isTimerActive(ReceiveConnAck))
        timer.startSingleTimer(ReceiveConnAck,
                               ReceiveConnAckTimeout(data.connectionId),
                               data.settings.receiveConnAckTimeout)
      Behaviors
        .receivePartial[Event] {
          case (context, connect @ ConnectReceivedLocally(connectionId, _, _, _))
              if connectionId != data.connectionId =>
            context.self ! connect
            disconnect(context, data.remote, data)
          case (_, event) if event.connectionId.nonEmpty && event.connectionId != data.connectionId =>
            Behaviors.same
          case (context, ConnAckReceivedFromRemote(_, connAck, local))
              if connAck.returnCode.contains(ConnAckReturnCode.ConnectionAccepted) =>
            local.success(ForwardConnAck(data.connectData))

            timer.cancel(ReceiveConnAck)

            BehaviorRunner.run(
              serverConnected(
                ConnAckReceived(
                  data.connectionId,
                  data.connect.connectFlags,
                  data.connect.keepAlive,
                  pendingPingResp = false,
                  data.remote,
                  Vector.empty,
                  data.activeConsumers,
                  data.activeProducers,
                  data.pendingLocalPublications,
                  data.pendingRemotePublications,
                  data.consumerPacketRouter,
                  data.producerPacketRouter,
                  data.subscriberPacketRouter,
                  data.unsubscriberPacketRouter,
                  data.settings
                )
              ),
              context,
              data.stash.map(BehaviorRunner.StoredMessage.apply)
            )

          case (context, ConnAckReceivedFromRemote(_, _, local)) =>
            local.success(ForwardConnAck(data.connectData))
            timer.cancel(ReceiveConnAck)
            disconnect(context, data.remote, data)
          case (context, ReceiveConnAckTimeout(_)) =>
            data.remote.fail(ConnectFailed)
            timer.cancel(ReceiveConnAck)
            disconnect(context, data.remote, data)

          case (_, RequestRouters(_, replyTo)) =>
            replyTo ! Routers(data.consumerPacketRouter,
                              data.producerPacketRouter,
                              data.subscriberPacketRouter,
                              data.unsubscriberPacketRouter)
            Behaviors.same

          case (context, ConnectionLost(_)) =>
            timer.cancel(ReceiveConnAck)
            disconnect(context, data.remote, data)

          case (_, e) =>
            serverConnect(data.copy(stash = data.stash :+ e))
        }
        .receiveSignal {
          case (_, _: Terminated) =>
            Behaviors.same
          case (_, PostStop) =>
            data.remote.complete()
            Behaviors.same
        }

  }

  def serverConnected(data: ConnAckReceived,
                      resetPingReqTimer: Boolean = true)(implicit mat: Materializer): Behavior[Event] =
    Behaviors.withTimers { timer =>
      val SendPingreq = "send-pingreq"
      if (resetPingReqTimer && data.keepAlive.toMillis > 0)
        timer.startSingleTimer(SendPingreq, SendPingReqTimeout(data.connectionId), data.keepAlive)

      Behaviors
        .receivePartial[Event] {
          case (context, connect @ ConnectReceivedLocally(connectionId, _, _, _))
              if connectionId != data.connectionId =>
            context.self ! connect
            disconnect(context, data.remote, data)

          case (_, event) if event.connectionId.nonEmpty && event.connectionId != data.connectionId =>
            Behaviors.same

          case (context, ConnectionLost(_)) =>
            timer.cancel(SendPingreq)
            disconnect(context, data.remote, data)

          case (context, DisconnectReceivedLocally(_, remote)) =>
            remote.success(ForwardDisconnect)
            timer.cancel(SendPingreq)
            disconnect(context, data.remote, data)

          case (context, SubscribeReceivedLocally(_, _, subscribeData, remote)) =>
            context.watch(
              context.spawnAnonymous(Subscriber(subscribeData, remote, data.subscriberPacketRouter, data.settings))
            )
            serverConnected(data)

          case (context, UnsubscribeReceivedLocally(_, _, unsubscribeData, remote)) =>
            context.watch(
              context
                .spawnAnonymous(Unsubscriber(unsubscribeData, remote, data.unsubscriberPacketRouter, data.settings))
            )
            serverConnected(data)

          case (_, PublishReceivedFromRemote(_, publish, local))
              if (publish.flags & ControlPacketFlags.QoSReserved).underlying == 0 =>
            local.success(Consumer.ForwardPublish)
            serverConnected(data, resetPingReqTimer = false)

          case (context,
                prfr @ PublishReceivedFromRemote(_, publish @ Publish(_, topicName, Some(packetId), _), local)) =>
            data.activeConsumers.get(topicName) match {
              case None =>
                val consumerName = ActorName.mkName(ConsumerNamePrefix + topicName + "-" + context.children.size)
                val consumer =
                  context.spawn(Consumer(publish, None, packetId, local, data.consumerPacketRouter, data.settings),
                                consumerName)
                context.watch(consumer)
                serverConnected(data.copy(activeConsumers = data.activeConsumers + (publish.topicName -> consumer)),
                                resetPingReqTimer = false)

              case Some(consumer) if publish.flags.contains(ControlPacketFlags.DUP) =>
                consumer ! Consumer.DupPublishReceivedFromRemote(local)
                serverConnected(data, resetPingReqTimer = false)

              case Some(_) =>
                serverConnected(
                  data.copy(pendingRemotePublications = data.pendingRemotePublications :+ (publish.topicName -> prfr)),
                  resetPingReqTimer = false
                )
            }

          case (context, ConsumerFree(topicName)) =>
            val i = data.pendingRemotePublications.indexWhere(_._1 == topicName)
            if (i >= 0) {
              val prfr = data.pendingRemotePublications(i)._2
              val consumerName = ActorName.mkName(ConsumerNamePrefix + topicName + "-" + context.children.size)
              val consumer = context.spawn(
                Consumer(prfr.publish,
                         None,
                         prfr.publish.packetId.get,
                         prfr.local,
                         data.consumerPacketRouter,
                         data.settings),
                consumerName
              )
              context.watch(consumer)
              serverConnected(
                data.copy(
                  activeConsumers = data.activeConsumers + (topicName -> consumer),
                  pendingRemotePublications =
                    data.pendingRemotePublications.take(i) ++ data.pendingRemotePublications.drop(i + 1)
                )
              )
            } else {
              serverConnected(data.copy(activeConsumers = data.activeConsumers - topicName))
            }

          case (context, PublishReceivedLocally(publish, _))
              if (publish.flags & ControlPacketFlags.QoSReserved).underlying == 0 =>
            QueueOfferState.waitForQueueOfferCompleted(
              data.remote.offer(ForwardPublish(publish, None)),
              result => QueueOfferCompleted(ByteString.empty, result.toEither),
              serverConnected(data),
              stash = Vector.empty
            )

          case (context, prl @ PublishReceivedLocally(publish, publishData)) =>
            val producerName = ActorName.mkName(ProducerNamePrefix + publish.topicName + "-" + context.children.size)
            if (!data.activeProducers.contains(publish.topicName)) {
              val reply = Promise[Source[Producer.ForwardPublishingCommand, NotUsed]]

              Source
                .fromFutureSource(reply.future)
                .runForeach(msg => context.self ! ReceivedProducerPublishingCommand(msg))

              val producer =
                context.spawn(Producer(publish, publishData, reply, data.producerPacketRouter, data.settings),
                              producerName)
              context.watch(producer)
              serverConnected(data.copy(activeProducers = data.activeProducers + (publish.topicName -> producer)))
            } else {
              serverConnected(
                data.copy(pendingLocalPublications = data.pendingLocalPublications :+ (publish.topicName -> prl))
              )
            }

          case (context, ProducerFree(topicName)) =>
            val i = data.pendingLocalPublications.indexWhere(_._1 == topicName)
            if (i >= 0) {
              val prl = data.pendingLocalPublications(i)._2
              val producerName = ActorName.mkName(ProducerNamePrefix + topicName + "-" + context.children.size)
              val reply = Promise[Source[Producer.ForwardPublishingCommand, NotUsed]]

              Source
                .fromFutureSource(reply.future)
                .runForeach(msg => context.self ! ReceivedProducerPublishingCommand(msg))

              val producer = context.spawn(
                Producer(prl.publish, prl.publishData, reply, data.producerPacketRouter, data.settings),
                producerName
              )
              context.watch(producer)
              serverConnected(
                data.copy(
                  activeProducers = data.activeProducers + (topicName -> producer),
                  pendingLocalPublications =
                    data.pendingLocalPublications.take(i) ++ data.pendingLocalPublications.drop(i + 1)
                )
              )
            } else {
              serverConnected(data.copy(activeProducers = data.activeProducers - topicName))
            }

          case (context, ReceivedProducerPublishingCommand(Producer.ForwardPublish(publish, packetId))) =>
            QueueOfferState.waitForQueueOfferCompleted(
              data.remote
                .offer(ForwardPublish(publish, packetId)),
              result => QueueOfferCompleted(ByteString.empty, result.toEither),
              serverConnected(data, resetPingReqTimer = false),
              stash = Vector.empty
            )

          case (context, ReceivedProducerPublishingCommand(Producer.ForwardPubRel(_, packetId))) =>
            QueueOfferState.waitForQueueOfferCompleted(
              data.remote
                .offer(ForwardPubRel(packetId)),
              result => QueueOfferCompleted(ByteString.empty, result.toEither),
              serverConnected(data, resetPingReqTimer = false),
              stash = Vector.empty
            )

          case (context, SendPingReqTimeout(_)) if data.pendingPingResp =>
            data.remote.fail(PingFailed)
            timer.cancel(SendPingreq)
            disconnect(context, data.remote, data)

          case (context, SendPingReqTimeout(_)) =>
            QueueOfferState.waitForQueueOfferCompleted(
              data.remote
                .offer(ForwardPingReq),
              result => QueueOfferCompleted(ByteString.empty, result.toEither),
              serverConnected(data.copy(pendingPingResp = true)),
              stash = Vector.empty
            )

          case (_, PingRespReceivedFromRemote(_, local)) =>
            local.success(ForwardPingResp)
            serverConnected(data.copy(pendingPingResp = false))

          case (_, RequestRouters(_, replyTo)) =>
            replyTo ! Routers(data.consumerPacketRouter,
                              data.producerPacketRouter,
                              data.subscriberPacketRouter,
                              data.unsubscriberPacketRouter)
            Behaviors.same

        }
        .receiveSignal {
          case (context, ChildFailed(_, failure))
              if failure == Subscriber.SubscribeFailed ||
              failure == Unsubscriber.UnsubscribeFailed =>
            data.remote.fail(failure)
            disconnect(context, data.remote, data)
          case (context, t: Terminated) =>
            data.activeConsumers.find(_._2 == t.ref) match {
              case Some((topic, _)) =>
                context.self ! ConsumerFree(topic)
              case None =>
                data.activeProducers.find(_._2 == t.ref) match {
                  case Some((topic, _)) =>
                    context.self ! ProducerFree(topic)
                  case None =>
                }
            }
            serverConnected(data)
          case (_, PostStop) =>
            data.remote.complete()
            Behaviors.same
        }
    }
}

/*
 * A subscriber manages the client state in relation to having made a
 * subscription to a server-side topic. A subscriber is created
 * per server per topic.
 */
@InternalApi private[streaming] object Subscriber {

  type SubscribeData = Option[_]

  /*
   * No ACK received - the subscription failed
   */
  case object SubscribeFailed extends Exception with NoStackTrace

  /*
   * Construct with the starting state
   */
  def apply(subscribeData: SubscribeData,
            remote: Promise[ForwardSubscribe],
            packetRouter: ActorRef[LocalPacketRouter.Request[Event]],
            settings: MqttSessionSettings): Behavior[Event] =
    prepareServerSubscribe(Start(subscribeData, remote, packetRouter, settings))

  // Our FSM data, FSM events and commands emitted by the FSM

  sealed abstract class Data(val settings: MqttSessionSettings)
  final case class Start(subscribeData: SubscribeData,
                         remote: Promise[ForwardSubscribe],
                         packetRouter: ActorRef[LocalPacketRouter.Request[Event]],
                         override val settings: MqttSessionSettings)
      extends Data(settings)
  final case class ServerSubscribe(packetId: PacketId,
                                   subscribeData: SubscribeData,
                                   packetRouter: ActorRef[LocalPacketRouter.Request[Event]],
                                   override val settings: MqttSessionSettings)
      extends Data(settings)

  sealed abstract class Event
  final case class AcquiredPacketId(packetId: PacketId) extends Event
  final case object UnobtainablePacketId extends Event
  final case class SubAckReceivedFromRemote(local: Promise[ForwardSubAck]) extends Event
  case object ReceiveSubAckTimeout extends Event

  sealed abstract class Command
  final case class ForwardSubscribe(packetId: PacketId) extends Command
  final case class ForwardSubAck(connectData: SubscribeData) extends Command

  // State event handling

  def prepareServerSubscribe(data: Start): Behavior[Event] = Behaviors.setup { context =>
    val reply = Promise[LocalPacketRouter.Registered]
    data.packetRouter ! LocalPacketRouter.Register(context.self, reply)
    import context.executionContext
    reply.future.onComplete {
      case Success(registered: LocalPacketRouter.Registered) => context.self ! AcquiredPacketId(registered.packetId)
      case Failure(_) => context.self ! UnobtainablePacketId
    }

    Behaviors.receiveMessagePartial[Event] {
      case AcquiredPacketId(packetId) =>
        data.remote.success(ForwardSubscribe(packetId))
        serverSubscribe(
          ServerSubscribe(packetId, data.subscribeData, data.packetRouter, data.settings)
        )
      case UnobtainablePacketId =>
        data.remote.failure(SubscribeFailed)
        throw SubscribeFailed
    }
  }

  def serverSubscribe(data: ServerSubscribe): Behavior[Event] = Behaviors.withTimers { timer =>
    val ReceiveSuback = "client-receive-suback"
    timer.startSingleTimer(ReceiveSuback, ReceiveSubAckTimeout, data.settings.receiveSubAckTimeout)

    Behaviors
      .receiveMessagePartial[Event] {
        case SubAckReceivedFromRemote(local) =>
          local.success(ForwardSubAck(data.subscribeData))
          Behaviors.stopped
        case ReceiveSubAckTimeout =>
          throw SubscribeFailed
      }
  }
}

/*
 * A unsubscriber manages the client state in relation to unsubscribing from a
 * server-side topic. A unsubscriber is created per server per topic.
 */
@InternalApi private[streaming] object Unsubscriber {

  /*
   * No ACK received - the unsubscription failed
   */
  case object UnsubscribeFailed extends Exception with NoStackTrace

  type UnsubscribeData = Option[_]

  /*
   * Construct with the starting state
   */
  def apply(unsubscribeData: UnsubscribeData,
            remote: Promise[ForwardUnsubscribe],
            packetRouter: ActorRef[LocalPacketRouter.Request[Event]],
            settings: MqttSessionSettings): Behavior[Event] =
    prepareServerUnsubscribe(Start(unsubscribeData, remote, packetRouter, settings))

  // Our FSM data, FSM events and commands emitted by the FSM

  sealed abstract class Data(val settings: MqttSessionSettings)
  final case class Start(unsubscribeData: UnsubscribeData,
                         remote: Promise[ForwardUnsubscribe],
                         packetRouter: ActorRef[LocalPacketRouter.Request[Event]],
                         override val settings: MqttSessionSettings)
      extends Data(settings)
  final case class ServerUnsubscribe(packetId: PacketId,
                                     unsubscribeData: UnsubscribeData,
                                     packetRouter: ActorRef[LocalPacketRouter.Request[Event]],
                                     override val settings: MqttSessionSettings)
      extends Data(settings)

  sealed abstract class Event
  final case class AcquiredPacketId(packetId: PacketId) extends Event
  final case object UnobtainablePacketId extends Event
  final case class UnsubAckReceivedFromRemote(local: Promise[ForwardUnsubAck]) extends Event
  case object ReceiveUnsubAckTimeout extends Event

  sealed abstract class Command
  final case class ForwardUnsubscribe(packetId: PacketId) extends Command
  final case class ForwardUnsubAck(connectData: UnsubscribeData) extends Command

  // State event handling

  def prepareServerUnsubscribe(data: Start): Behavior[Event] = Behaviors.setup { context =>
    val reply = Promise[LocalPacketRouter.Registered]
    data.packetRouter ! LocalPacketRouter.Register(context.self, reply)
    import context.executionContext
    reply.future.onComplete {
      case Success(registered: LocalPacketRouter.Registered) => context.self ! AcquiredPacketId(registered.packetId)
      case Failure(_) => context.self ! UnobtainablePacketId
    }

    Behaviors.receiveMessagePartial[Event] {
      case AcquiredPacketId(packetId) =>
        data.remote.success(ForwardUnsubscribe(packetId))
        serverUnsubscribe(
          ServerUnsubscribe(packetId, data.unsubscribeData, data.packetRouter, data.settings)
        )
      case UnobtainablePacketId =>
        data.remote.failure(UnsubscribeFailed)
        throw UnsubscribeFailed
    }
  }

  def serverUnsubscribe(data: ServerUnsubscribe): Behavior[Event] = Behaviors.withTimers { timer =>
    val ReceiveUnsubAck = "client-receive-unsubAck"
    timer.startSingleTimer(ReceiveUnsubAck, ReceiveUnsubAckTimeout, data.settings.receiveUnsubAckTimeout)

    Behaviors
      .receiveMessagePartial[Event] {
        case UnsubAckReceivedFromRemote(local) =>
          local.success(ForwardUnsubAck(data.unsubscribeData))
          Behaviors.stopped
        case ReceiveUnsubAckTimeout =>
          throw UnsubscribeFailed
      }
  }
}
