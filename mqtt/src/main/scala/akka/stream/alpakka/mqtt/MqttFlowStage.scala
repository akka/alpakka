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
import org.eclipse.paho.client.mqttv3.{IMqttAsyncClient, IMqttToken, MqttMessage => PahoMqttMessage}

import scala.collection.mutable
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

object MqttFlowStage {
  final object NoClientException extends Exception("No MQTT client.")
}

final class MqttFlowStage(sourceSettings: MqttSourceSettings,
                          bufferSize: Int,
                          qos: MqttQoS,
                          manualAcks: Boolean = false)
    extends GraphStageWithMaterializedValue[FlowShape[MqttMessage, MqttCommittableMessage], Future[Done]] {
  import MqttFlowStage.NoClientException
  import MqttConnectorLogic._

  private val in = Inlet[MqttMessage](s"MqttFlow.in")
  private val out = Outlet[MqttCommittableMessage](s"MqttFlow.out")
  override val shape: Shape = FlowShape(in, out)
  override protected def initialAttributes: Attributes = Attributes.name("MqttFlow")

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) = {
    val subscriptionPromise = Promise[Done]

    (new GraphStageLogic(shape) with MqttConnectorLogic {
      private val backpressure = new Semaphore(bufferSize)
      private val queue = mutable.Queue[MqttCommittableMessage]()
      private val unackedMessages = new AtomicInteger()

      private var mqttClient: Option[IMqttAsyncClient] = None

      private val mqttSubscriptionCallback: Try[IMqttToken] => Unit = { conn =>
        subscriptionPromise.complete(conn.map { _ =>
          Done
        })
        pull(in)
      }

      private val onMessage = getAsyncCallback[MqttCommittableMessage] { message =>
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

      override val connectionSettings: MqttConnectionSettings = sourceSettings.connectionSettings

      setHandler(
        in,
        new InHandler {
          override def onPush(): Unit =
            mqttClient match {
              case Some(client) =>
                val msg = grab(in)
                val pahoMsg = new PahoMqttMessage(msg.payload.toArray)
                pahoMsg.setQos(qos.byteValue)
                client.publish(msg.topic, pahoMsg, msg, onPublished.invoke _)
              case None => failStage(NoClientException)
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

      override def handleConnection(client: IMqttAsyncClient): Unit = {
        if (manualAcks) client.setManualAcks(true)
        val (topics, qos) = sourceSettings.subscriptions.unzip
        mqttClient = Some(client)
        if (topics.nonEmpty) {
          client.subscribe(topics.toArray, qos.map(_.byteValue.toInt).toArray, (), mqttSubscriptionCallback)
        } else {
          subscriptionPromise.complete(Success(Done))
          pull(in)
        }
      }

      override def onMessage(message: MqttCommittableMessage): Unit = {
        backpressure.acquire()
        onMessage.invoke(message)
      }

      override def commitCallback(args: CommitCallbackArguments): Unit =
        try {
          mqttClient.get.messageArrivedComplete(args.messageId, args.qos.byteValue.toInt)
          if (unackedMessages.decrementAndGet() == 0 && (isClosed(out) || (isClosed(in) && queue.isEmpty)))
            completeStage()
          args.promise.complete(Try(Done))
        } catch {
          case e: Throwable => args.promise.failure(e)
        }

      def pushMessage(message: MqttCommittableMessage): Unit = {
        push(out, message)
        backpressure.release()
        if (manualAcks) unackedMessages.incrementAndGet()
      }

      override def handleConnectionLost(ex: Throwable): Unit = {
        failStage(ex)
        subscriptionPromise.tryFailure(ex)
      }

      override def postStop(): Unit = {
        if (!subscriptionPromise.isCompleted)
          subscriptionPromise
            .tryFailure(
              new IllegalStateException("Cannot complete subscription because the stage is about to stop or fail")
            )
        mqttClient.foreach {
          case c if c.isConnected => c.disconnect()
          case _ => ()
        }
      }
    }, subscriptionPromise.future)
  }
}
