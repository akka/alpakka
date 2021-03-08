/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.impl

import java.util.Properties
import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicInteger

import akka.Done
import akka.annotation.InternalApi
import akka.stream.{Shape, _}
import akka.stream.alpakka.mqtt._
import akka.stream.alpakka.mqtt.scaladsl.MqttMessageWithAck
import akka.stream.stage._
import akka.util.ByteString
import org.eclipse.paho.client.mqttv3.{
  IMqttActionListener,
  IMqttAsyncClient,
  IMqttDeliveryToken,
  IMqttToken,
  MqttAsyncClient,
  MqttCallbackExtended,
  MqttConnectOptions,
  MqttException,
  MqttMessage => PahoMqttMessage
}

import scala.collection.mutable
import scala.concurrent.{Future, Promise}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}
import org.eclipse.paho.client.mqttv3.DisconnectedBufferOptions
import akka.stream.alpakka.mqtt.MqttOfflinePersistenceSettings

/**
 * INTERNAL API
 */
@InternalApi
private[mqtt] final class MqttFlowStage(connectionSettings: MqttConnectionSettings,
                                        subscriptions: Map[String, MqttQoS],
                                        bufferSize: Int,
                                        defaultQoS: MqttQoS,
                                        manualAcks: Boolean = false)
    extends GraphStageWithMaterializedValue[FlowShape[MqttMessage, MqttMessageWithAck], Future[Done]] {

  private val in = Inlet[MqttMessage]("MqttFlow.in")
  private val out = Outlet[MqttMessageWithAck]("MqttFlow.out")
  override val shape: Shape = FlowShape(in, out)

  override protected def initialAttributes: Attributes = Attributes.name("MqttFlow")

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) = {
    val subscriptionPromise = Promise[Done]
    val logic = new MqttFlowStageLogic[MqttMessage](in,
                                                    out,
                                                    shape,
                                                    subscriptionPromise,
                                                    connectionSettings,
                                                    subscriptions,
                                                    bufferSize,
                                                    defaultQoS,
                                                    manualAcks) {

      override def publishPending(msg: MqttMessage): Unit = super.publishToMqtt(msg)

    }
    (logic, subscriptionPromise.future)
  }
}

abstract class MqttFlowStageLogic[I](in: Inlet[I],
                                     out: Outlet[MqttMessageWithAck],
                                     shape: Shape,
                                     subscriptionPromise: Promise[Done],
                                     connectionSettings: MqttConnectionSettings,
                                     subscriptions: Map[String, MqttQoS],
                                     bufferSize: Int,
                                     defaultQoS: MqttQoS,
                                     manualAcks: Boolean)
    extends GraphStageLogic(shape)
    with StageLogging
    with InHandler
    with OutHandler {

  import akka.stream.alpakka.mqtt.impl.MqttFlowStageLogic._

  private val backpressurePahoClient = new Semaphore(bufferSize)
  private var pendingMsg = Option.empty[I]
  private val queue = mutable.Queue[MqttMessageWithAck]()
  private val unackedMessages = new AtomicInteger()

  protected def handleDeliveryComplete(token: IMqttDeliveryToken): Unit = ()

  private val onSubscribe: AsyncCallback[Try[IMqttToken]] = getAsyncCallback[Try[IMqttToken]] { conn =>
    if (subscriptionPromise.isCompleted) {
      log.debug("subscription re-established")
    } else {
      subscriptionPromise.complete(conn.map(_ => {
        log.debug("subscription established")
        Done
      }))
      pull(in)
    }
  }

  private val onConnect: AsyncCallback[IMqttAsyncClient] =
    getAsyncCallback[IMqttAsyncClient]((client: IMqttAsyncClient) => {
      log.debug("connected")
      if (subscriptions.nonEmpty) {
        if (manualAcks) client.setManualAcks(true)
        val (topics, qoses) = subscriptions.unzip
        client.subscribe(topics.toArray, qoses.map(_.value).toArray, (), asActionListener(onSubscribe.invoke))
      } else {
        subscriptionPromise.complete(SuccessfullyDone)
        pull(in)
      }
    })

  private val onConnectionLost: AsyncCallback[Throwable] = getAsyncCallback[Throwable](failStageWith)

  private val onMessageAsyncCallback: AsyncCallback[MqttMessageWithAck] =
    getAsyncCallback[MqttMessageWithAck] { message =>
      if (isAvailable(out)) {
        pushDownstream(message)
      } else if (queue.size + 1 > bufferSize) {
        failStageWith(new RuntimeException(s"Reached maximum buffer size $bufferSize"))
      } else {
        queue.enqueue(message)
      }
    }

  private val onPublished: AsyncCallback[Try[IMqttToken]] = getAsyncCallback[Try[IMqttToken]] {
    case Success(_) => if (!hasBeenPulled(in)) pull(in)
    case Failure(ex) => failStageWith(ex)
  }

  private def createPahoBufferOptions(settings: MqttOfflinePersistenceSettings): DisconnectedBufferOptions = {

    val disconnectedBufferOptions = new DisconnectedBufferOptions()

    disconnectedBufferOptions.setBufferEnabled(true)
    disconnectedBufferOptions.setBufferSize(settings.bufferSize)
    disconnectedBufferOptions.setDeleteOldestMessages(settings.deleteOldestMessage)
    disconnectedBufferOptions.setPersistBuffer(settings.persistBuffer)

    disconnectedBufferOptions
  }

  private val client = new MqttAsyncClient(
    connectionSettings.broker,
    connectionSettings.clientId,
    connectionSettings.persistence
  )

  private def mqttClient =
    connectionSettings.offlinePersistenceSettings match {
      case Some(bufferOpts) =>
        client.setBufferOpts(createPahoBufferOptions(bufferOpts))

        client
      case _ => client
    }

  private val commitCallback: AsyncCallback[CommitCallbackArguments] =
    getAsyncCallback[CommitCallbackArguments](
      (args: CommitCallbackArguments) =>
        try {
          mqttClient.messageArrivedComplete(args.messageId, args.qos.value)
          if (unackedMessages.decrementAndGet() == 0 && (isClosed(out) || (isClosed(in) && queue.isEmpty)))
            completeStage()
          args.promise.complete(SuccessfullyDone)
        } catch {
          case e: Throwable => args.promise.failure(e)
        }
    )

  mqttClient.setCallback(new MqttCallbackExtended {
    override def messageArrived(topic: String, pahoMessage: PahoMqttMessage): Unit = {
      backpressurePahoClient.acquire()
      val message = new MqttMessageWithAck {
        override val message = MqttMessage(topic, ByteString.fromArrayUnsafe(pahoMessage.getPayload))

        override def ack(): Future[Done] = {
          val promise = Promise[Done]()
          val qos = pahoMessage.getQos match {
            case 0 => MqttQoS.AtMostOnce
            case 1 => MqttQoS.AtLeastOnce
            case 2 => MqttQoS.ExactlyOnce
          }
          commitCallback.invoke(CommitCallbackArguments(pahoMessage.getId, qos, promise))
          promise.future
        }
      }
      onMessageAsyncCallback.invoke(message)
    }

    override def deliveryComplete(token: IMqttDeliveryToken): Unit = handleDeliveryComplete(token)

    override def connectionLost(cause: Throwable): Unit =
      if (!connectionSettings.automaticReconnect) {
        log.info("connection lost (you might want to enable `automaticReconnect` in `MqttConnectionSettings`)")
        onConnectionLost.invoke(cause)
      } else {
        log.info("connection lost, trying to reconnect")
      }

    override def connectComplete(reconnect: Boolean, serverURI: String): Unit = {
      pendingMsg.foreach { msg =>
        publishPending(msg)
        pendingMsg = None
      }
      if (reconnect && !hasBeenPulled(in)) pull(in)
    }
  })

  // InHandler
  override def onPush(): Unit = {
    val msg = grab(in)
    try {
      publishPending(msg)
    } catch {
      case _: MqttException if connectionSettings.automaticReconnect => pendingMsg = Some(msg)
      case NonFatal(e) => throw e
    }
  }

  override def onUpstreamFinish(): Unit = {
    setKeepGoing(true)
    if (queue.isEmpty && unackedMessages.get() == 0) super.onUpstreamFinish()
  }

  override def onUpstreamFailure(ex: Throwable): Unit = {
    setKeepGoing(true)
    if (queue.isEmpty && unackedMessages.get() == 0) super.onUpstreamFailure(ex)
  }

  // OutHandler
  override def onPull(): Unit =
    if (queue.nonEmpty) {
      pushDownstream(queue.dequeue())
      if (unackedMessages.get() == 0 && isClosed(in)) completeStage()
    }

  override def onDownstreamFinish(cause: Throwable): Unit = {
    setKeepGoing(true)
    if (unackedMessages.get() == 0) super.onDownstreamFinish(cause)
  }

  setHandlers(in, out, this)

  def publishToMqtt(msg: MqttMessage): IMqttDeliveryToken = {
    val pahoMsg = new PahoMqttMessage(msg.payload.toArray)
    pahoMsg.setQos(msg.qos.getOrElse(defaultQoS).value)
    pahoMsg.setRetained(msg.retained)
    mqttClient.publish(msg.topic, pahoMsg, msg, asActionListener(onPublished.invoke))
  }

  def publishPending(msg: I): Unit = ()

  private def pushDownstream(message: MqttMessageWithAck): Unit = {
    push(out, message)
    backpressurePahoClient.release()
    if (manualAcks) unackedMessages.incrementAndGet()
  }

  private def failStageWith(ex: Throwable): Unit = {
    subscriptionPromise.tryFailure(ex)
    failStage(ex)
  }

  override def preStart(): Unit =
    try {
      mqttClient.connect(
        asConnectOptions(connectionSettings),
        (),
        new IMqttActionListener {
          override def onSuccess(v: IMqttToken): Unit = onConnect.invoke(v.getClient)

          override def onFailure(asyncActionToken: IMqttToken, ex: Throwable): Unit = onConnectionLost.invoke(ex)
        }
      )
    } catch {
      case e: Throwable => failStageWith(e)
    }

  override def postStop(): Unit = {
    if (!subscriptionPromise.isCompleted)
      subscriptionPromise
        .tryFailure(
          new IllegalStateException("Cannot complete subscription because the stage is about to stop or fail")
        )

    try {
      log.debug("stage stopped, disconnecting")
      mqttClient.disconnect(
        connectionSettings.disconnectQuiesceTimeout.toMillis,
        null,
        new IMqttActionListener {
          override def onSuccess(asyncActionToken: IMqttToken): Unit = mqttClient.close()

          override def onFailure(asyncActionToken: IMqttToken, exception: Throwable): Unit = {
            // Use 0 quiesce timeout as we have already quiesced in `disconnect`
            mqttClient.disconnectForcibly(0, connectionSettings.disconnectTimeout.toMillis)
            // Only disconnected client can be closed
            mqttClient.close()
          }
        }
      )
    } catch {
      // Not to worry - disconnect is best effort - don't worry if already disconnected
      case _: MqttException =>
        try {
          mqttClient.close()
        } catch {
          case _: MqttException =>
        }
    }
  }
}

/**
 * INTERNAL API
 */
@InternalApi
private[mqtt] object MqttFlowStageLogic {
  private val SuccessfullyDone = Success(Done)

  final private case class CommitCallbackArguments(messageId: Int, qos: MqttQoS, promise: Promise[Done])

  def asConnectOptions(connectionSettings: MqttConnectionSettings): MqttConnectOptions = {
    val options = new MqttConnectOptions
    connectionSettings.auth.foreach {
      case (user, password) =>
        options.setUserName(user)
        options.setPassword(password.toCharArray)
    }
    connectionSettings.socketFactory.foreach(options.setSocketFactory)
    connectionSettings.will.foreach { will =>
      options.setWill(
        will.topic,
        will.payload.toArray,
        will.qos.getOrElse(MqttQoS.atLeastOnce).value,
        will.retained
      )
    }
    options.setCleanSession(connectionSettings.cleanSession)
    options.setAutomaticReconnect(connectionSettings.automaticReconnect)
    options.setKeepAliveInterval(connectionSettings.keepAliveInterval.toSeconds.toInt)
    options.setConnectionTimeout(connectionSettings.connectionTimeout.toSeconds.toInt)
    options.setMaxInflight(connectionSettings.maxInFlight)
    options.setMqttVersion(connectionSettings.mqttVersion)
    if (connectionSettings.serverUris.nonEmpty) {
      options.setServerURIs(connectionSettings.serverUris.toArray)
    }
    connectionSettings.sslHostnameVerifier.foreach(options.setSSLHostnameVerifier)
    if (connectionSettings.sslProperties.nonEmpty) {
      val properties = new Properties()
      connectionSettings.sslProperties.foreach { case (key, value) => properties.setProperty(key, value) }
      options.setSSLProperties(properties)
    }
    options
  }

  def asActionListener(func: Try[IMqttToken] => Unit): IMqttActionListener = new IMqttActionListener {
    def onSuccess(token: IMqttToken): Unit = func(Success(token))

    def onFailure(token: IMqttToken, ex: Throwable): Unit = func(Failure(ex))
  }
}
