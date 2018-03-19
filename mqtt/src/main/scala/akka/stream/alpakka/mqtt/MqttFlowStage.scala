/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt

import java.util.Properties
import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicInteger

import akka.Done
import akka.stream._
import akka.stream.stage._
import akka.stream.alpakka.mqtt.scaladsl.MqttCommittableMessage
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

final class MqttFlowStage(sourceSettings: MqttSourceSettings,
                          bufferSize: Int,
                          qos: MqttQoS,
                          manualAcks: Boolean = false)
    extends GraphStageWithMaterializedValue[FlowShape[MqttMessage, MqttCommittableMessage], Future[Done]] {
  import MqttConnectorLogic._

  private val in = Inlet[MqttMessage](s"MqttFlow.in")
  private val out = Outlet[MqttCommittableMessage](s"MqttFlow.out")
  override val shape: Shape = FlowShape(in, out)
  override protected def initialAttributes: Attributes = Attributes.name("MqttFlow")

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) = {
    val subscriptionPromise = Promise[Done]

    (new GraphStageLogic(shape) {
      private val backpressure = new Semaphore(bufferSize)
      private var pendingMsg = Option.empty[MqttMessage]
      private val queue = mutable.Queue[MqttCommittableMessage]()
      private val unackedMessages = new AtomicInteger()

      private val mqttSubscriptionCallback: Try[IMqttToken] => Unit = { conn =>
        subscriptionPromise.complete(conn.map { _ =>
          Done
        })
        pull(in)
      }

      private def onConnect =
        getAsyncCallback[IMqttAsyncClient]((client: IMqttAsyncClient) => {
          if (manualAcks) client.setManualAcks(true)
          val (topics, qoses) = sourceSettings.subscriptions.unzip
          if (topics.nonEmpty) {
            client.subscribe(topics.toArray, qoses.map(_.byteValue.toInt).toArray, (), mqttSubscriptionCallback)
          } else {
            subscriptionPromise.complete(Success(Done))
            pull(in)
          }
        })

      private def onConnectionLost = getAsyncCallback[Throwable](onFailure)

      private def onMessage(message: MqttCommittableMessage): Unit = {
        backpressure.acquire()
        onMessageAsyncCallback.invoke(message)
      }

      private val onMessageAsyncCallback = getAsyncCallback[MqttCommittableMessage] { message =>
        if (isAvailable(out)) {
          pushMessage(message)
        } else if (queue.size + 1 > bufferSize) {
          onFailure(new RuntimeException(s"Reached maximum buffer size $bufferSize"))
        } else {
          queue.enqueue(message)
        }
      }

      private val onPublished = getAsyncCallback[Try[IMqttToken]] {
        case Success(_) => if (!hasBeenPulled(in)) pull(in)
        case Failure(ex) => onFailure(ex)
      }

      private def commitCallback =
        getAsyncCallback[CommitCallbackArguments](
          (args: CommitCallbackArguments) =>
            try {
              mqttClient.messageArrivedComplete(args.messageId, args.qos.byteValue.toInt)
              if (unackedMessages.decrementAndGet() == 0 && (isClosed(out) || (isClosed(in) && queue.isEmpty)))
                completeStage()
              args.promise.complete(Try(Done))
            } catch {
              case e: Throwable => args.promise.failure(e)
          }
        )

      val connectionSettings: MqttConnectionSettings = sourceSettings.connectionSettings
      val mqttClient = new MqttAsyncClient(
        connectionSettings.broker,
        connectionSettings.clientId,
        connectionSettings.persistence
      )

      private def publishMsg(msg: MqttMessage) = {
        val pahoMsg = new PahoMqttMessage(msg.payload.toArray)
        pahoMsg.setQos(msg.qos.getOrElse(qos).byteValue)
        pahoMsg.setRetained(msg.retained)
        mqttClient.publish(msg.topic, pahoMsg, msg, onPublished.invoke _)
      }

      mqttClient.setCallback(new MqttCallbackExtended {
        override def messageArrived(topic: String, pahoMessage: PahoMqttMessage): Unit =
          onMessage(new MqttCommittableMessage {
            override val message = MqttMessage(topic, ByteString(pahoMessage.getPayload))
            override def messageArrivedComplete(): Future[Done] = {
              val promise = Promise[Done]()
              val qos = pahoMessage.getQos match {
                case 0 => MqttQoS.atMostOnce
                case 1 => MqttQoS.atLeastOnce
                case 2 => MqttQoS.exactlyOnce
              }
              commitCallback.invoke(CommitCallbackArguments(pahoMessage.getId, qos, promise))
              promise.future
            }
          })

        override def deliveryComplete(token: IMqttDeliveryToken): Unit = ()

        override def connectionLost(cause: Throwable): Unit =
          if (!connectOptions.isAutomaticReconnect) onConnectionLost.invoke(cause)

        override def connectComplete(reconnect: Boolean, serverURI: String): Unit = {
          pendingMsg.foreach { msg =>
            publishMsg(msg)
            pendingMsg = None
          }
          if (reconnect && !hasBeenPulled(in)) pull(in)
        }
      })

      val connectOptions: MqttConnectOptions = {
        val options = new MqttConnectOptions
        connectionSettings.auth.foreach {
          case (user, password) =>
            options.setUserName(user)
            options.setPassword(password.toCharArray)
        }
        connectionSettings.socketFactory.foreach { socketFactory =>
          options.setSocketFactory(socketFactory)
        }
        connectionSettings.will.foreach { will =>
          options.setWill(
            will.topic,
            will.payload.toArray,
            will.qos.getOrElse(MqttQoS.atLeastOnce).byteValue.toInt,
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
        connectionSettings.sslHostnameVerifier.foreach { sslHostnameVerifier =>
          options.setSSLHostnameVerifier(sslHostnameVerifier)
        }
        if (connectionSettings.sslProperties.nonEmpty) {
          val properties = new Properties()
          connectionSettings.sslProperties foreach { case (key, value) => properties.setProperty(key, value) }
          options.setSSLProperties(properties)
        }
        options
      }

      setHandler(
        in,
        new InHandler {
          override def onPush(): Unit = {
            val msg = grab(in)
            try {
              publishMsg(msg)
            } catch {
              case _: MqttException if connectOptions.isAutomaticReconnect => pendingMsg = Some(msg)
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
        }
      )

      setHandler(
        out,
        new OutHandler {
          override def onPull(): Unit =
            if (queue.nonEmpty) {
              pushMessage(queue.dequeue())
              if (unackedMessages.get() == 0 && isClosed(in)) completeStage()
            }

          override def onDownstreamFinish(): Unit = {
            setKeepGoing(true)
            if (unackedMessages.get() == 0) super.onDownstreamFinish()
          }
        }
      )

      private def pushMessage(message: MqttCommittableMessage): Unit = {
        push(out, message)
        backpressure.release()
        if (manualAcks) unackedMessages.incrementAndGet()
      }

      private def onFailure(ex: Throwable): Unit = {
        subscriptionPromise.tryFailure(ex)
        failStage(ex)
      }

      override def preStart(): Unit =
        try {
          mqttClient.connect(
            connectOptions,
            (),
            (token: Try[IMqttToken]) =>
              token match {
                case Success(v) => onConnect.invoke(v.getClient)
                case Failure(ex) => onConnectionLost.invoke(ex)
            }
          )
        } catch {
          case e: Throwable => onFailure(e)
        }

      override def postStop(): Unit = {
        if (!subscriptionPromise.isCompleted)
          subscriptionPromise
            .tryFailure(
              new IllegalStateException("Cannot complete subscription because the stage is about to stop or fail")
            )

        try {
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
    }, subscriptionPromise.future)
  }
}
