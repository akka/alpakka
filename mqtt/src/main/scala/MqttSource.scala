/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.contrib.mqtt

import akka.NotUsed
import akka.stream.scaladsl._
import akka.stream.stage._
import akka.stream._
import scala.collection.mutable
import scala.concurrent._
import org.eclipse.paho.client.mqttv3.IMqttAsyncClient
import org.eclipse.paho.client.mqttv3.IMqttToken
import scala.util.Try

object MqttSource {

  /**
   * Scala API:
   */
  def apply(settings: MqttSourceSettings, bufferSize: Int): Source[MqttMessage, Future[NotUsed]] =
    Source.fromGraph(new MqttSource(settings, bufferSize))

  /**
   * Java API:
   */
  def create(settings: MqttSourceSettings, bufferSize: Int): akka.stream.javadsl.Source[MqttMessage, Future[NotUsed]] =
    akka.stream.javadsl.Source.fromGraph(new MqttSource(settings, bufferSize))
}

final class MqttSource(settings: MqttSourceSettings, bufferSize: Int) extends GraphStageWithMaterializedValue[SourceShape[MqttMessage], Future[NotUsed]] {

  import MqttConnectorLogic._

  val out = Outlet[MqttMessage]("MqttSource.out")
  override val shape: SourceShape[MqttMessage] = SourceShape.of(out)
  override protected def initialAttributes: Attributes = Attributes.name("MqttSource")

  private val subscriptionPromise = Promise[NotUsed]

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes) = (new GraphStageLogic(shape) with MqttConnectorLogic {

    override val connectionSettings = settings.connectionSettings

    override val onConnect = getAsyncCallback[IMqttAsyncClient](handleConnection)
    override val onMessage = getAsyncCallback[MqttMessage](handleMessage)
    override val onConnectionLost = getAsyncCallback[Throwable](handleConnectionLost)

    private val queue = mutable.Queue[MqttMessage]()
    private val mqttSubscriptionCallback: Try[IMqttToken] => Unit = conn =>
      subscriptionPromise.complete(conn.map { _ => NotUsed })

    setHandler(out, new OutHandler {
      override def onPull(): Unit = {
        if (queue.nonEmpty) {
          pushAndAckMessage(queue.dequeue())
        }
      }
    })

    def handleConnection(client: IMqttAsyncClient) = {
      val (topics, qos) = settings.topics.unzip
      client.subscribe(topics.toArray, qos.toArray, (), mqttSubscriptionCallback)
    }

    def handleMessage(message: MqttMessage): Unit = {
      if (isAvailable(out)) {
        pushAndAckMessage(message)
      } else {
        if (queue.size + 1 > bufferSize) {
          failStage(new RuntimeException(s"Reached maximum buffer size $bufferSize"))
        } else {
          queue.enqueue(message)
        }
      }
    }

    def pushAndAckMessage(message: MqttMessage): Unit = {
      push(out, message)
    }

    def handleConnectionLost(ex: Throwable) =
      failStage(ex)

  }, subscriptionPromise.future)

}
