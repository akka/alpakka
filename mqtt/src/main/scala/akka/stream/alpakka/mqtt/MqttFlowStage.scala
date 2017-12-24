/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt

import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicInteger

import akka.Done
import akka.stream._
import akka.stream.stage._
import akka.stream.alpakka.mqtt.scaladsl.MqttCommittableMessage
import akka.util.ByteString
import org.eclipse.paho.client.mqttv3.{
  IMqttAsyncClient,
  IMqttDeliveryToken,
  IMqttToken,
  MqttAsyncClient,
  MqttCallbackExtended,
  MqttConnectOptions,
  MqttMessage => PahoMqttMessage
}

import scala.collection.mutable
import scala.concurrent.{Future, Promise}
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
          val (topics, qos) = sourceSettings.subscriptions.unzip
          if (topics.nonEmpty) {
            client.subscribe(topics.toArray, qos.map(_.byteValue.toInt).toArray, (), mqttSubscriptionCallback)
          } else {
            subscriptionPromise.complete(Success(Done))
            pull(in)
          }
        })

      private def onConnectionLost =
        getAsyncCallback[Throwable]((ex: Throwable) => {
          failStage(ex)
          subscriptionPromise.tryFailure(ex)
        })

      private def onMessage(message: MqttCommittableMessage): Unit = {
        backpressure.acquire()
        onMessageAsyncCallback.invoke(message)
      }

      private val onMessageAsyncCallback = getAsyncCallback[MqttCommittableMessage] { message =>
        if (isAvailable(out)) {
          pushMessage(message)
        } else if (queue.size + 1 > bufferSize) {
          failStage(new RuntimeException(s"Reached maximum buffer size $bufferSize"))
        } else {
          queue.enqueue(message)
        }
      }

      private val onPublished = getAsyncCallback[Try[IMqttToken]] {
        case Success(_) => pull(in)
        case Failure(ex) => failStage(ex)
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

      mqttClient.setCallback(new MqttCallbackExtended {
        override def messageArrived(topic: String, pahoMessage: PahoMqttMessage) =
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

        override def deliveryComplete(token: IMqttDeliveryToken) = ()

        override def connectionLost(cause: Throwable) =
          onConnectionLost.invoke(cause)

        override def connectComplete(reconnect: Boolean, serverURI: String) = if (reconnect) pull(in)
      })

      val connectOptions = {
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
        options
      }

      setHandler(
        in,
        new InHandler {
          override def onPush(): Unit = {
            val msg = grab(in)
            val pahoMsg = new PahoMqttMessage(msg.payload.toArray)
            pahoMsg.setQos(msg.qos.getOrElse(qos).byteValue)
            pahoMsg.setRetained(msg.retained)
            mqttClient.publish(msg.topic, pahoMsg, msg, onPublished.invoke _)
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

      override def preStart(): Unit =
        mqttClient.connect(
          connectOptions,
          (),
          (token: Try[IMqttToken]) =>
            token match {
              case Success(token) => onConnect.invoke(token.getClient)
              case Failure(ex) => onConnectionLost.invoke(ex)
          }
        )

      override def postStop(): Unit = {
        if (!subscriptionPromise.isCompleted)
          subscriptionPromise
            .tryFailure(
              new IllegalStateException("Cannot complete subscription because the stage is about to stop or fail")
            )
        try mqttClient.disconnect()
        catch { case _: Throwable => () }
      }
    }, subscriptionPromise.future)
  }
}
