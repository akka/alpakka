/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package jms

// #sample
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.ws.{WebSocketRequest, WebSocketUpgradeResponse}
import akka.stream.KillSwitch
import akka.stream.alpakka.jms.JmsConsumerSettings
import akka.stream.alpakka.jms.scaladsl.JmsConsumer
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}

import scala.collection.immutable.Seq
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
// #sample
import playground.{ActiveMqBroker, WebServer}

object JmsToWebSocket extends JmsSampleBase with App {

  ActiveMqBroker.start()
  WebServer.start()

  val connectionFactory = ActiveMqBroker.createConnectionFactory
  enqueue(connectionFactory)("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k")

  def wsMessageToString: ws.Message => Future[String] = {
    case message: ws.TextMessage.Strict =>
      Future.successful(message.text)

    case message: ws.TextMessage.Streamed =>
      val seq: Future[Seq[String]] = message.textStream.runWith(Sink.seq)
      seq.map(seq => seq.mkString)

    case message =>
      Future.successful(message.toString)
  }

  // format: off
  // #sample

  val jmsSource: Source[String, KillSwitch] =
    JmsConsumer.textSource(                                                           // (1)
      JmsConsumerSettings(connectionFactory).withBufferSize(10).withQueue("test")
    )

  val webSocketFlow: Flow[ws.Message, ws.Message, Future[WebSocketUpgradeResponse]] = // (2)
    Http().webSocketClientFlow(WebSocketRequest("ws://localhost:8080/webSocket/ping"))

  val (runningSource, wsUpgradeResponse): (KillSwitch, Future[WebSocketUpgradeResponse]) =
                                                     // stream element type
    jmsSource                                        //: String
      .map(ws.TextMessage(_))                        //: ws.TextMessage                  (3)
      .viaMat(webSocketFlow)(Keep.both)              //: ws.TextMessage                  (4)
      .mapAsync(1)(wsMessageToString)                //: String                          (5)
      .map("client received: " + _)                  //: String                          (6)
      .toMat(Sink.foreach(println))(Keep.left)       //                                  (7)
      .run()
  // #sample
  // format: on

  wsUpgradeResponse
    .map { upgrade =>
      if (upgrade.response.status == StatusCodes.SwitchingProtocols) {
        "WebSocket established"
      } else {
        throw new RuntimeException(s"Connection failed: ${upgrade.response.status}")
      }
    }
    .onComplete(println)

  wait(5.seconds)
  runningSource.shutdown()

  for {
    _ <- actorSystem.terminate()
    _ <- WebServer.stop()
    _ <- ActiveMqBroker.stop()
  } ()

}
