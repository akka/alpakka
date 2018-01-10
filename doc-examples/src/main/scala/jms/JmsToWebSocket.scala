/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package jms

// #sample
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.ws.{WebSocketRequest, WebSocketUpgradeResponse}
import akka.stream.alpakka.jms.JmsSourceSettings
import akka.stream.alpakka.jms.scaladsl.JmsSource
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.{Done, NotUsed}
import playground.{ActiveMqBroker, WebServer}

import scala.collection.immutable.Seq
import scala.concurrent.Future
// #sample

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

  val jmsSource: Source[String, _] =
    JmsSource.textSource(                                                             // (1)
      JmsSourceSettings(connectionFactory).withBufferSize(10).withQueue("test")
    )

  val webSocketFlow: Flow[ws.Message, ws.Message, Future[WebSocketUpgradeResponse]] = // (2)
    Http().webSocketClientFlow(WebSocketRequest("ws://localhost:8080/webSocket/ping"))

  val (wsUpgradeResponse, finished): (Future[WebSocketUpgradeResponse], Future[Done]) =
    jmsSource                                        //: String
      .map(ws.TextMessage(_))                        //: ws.TextMessage                  (3)
      .viaMat(webSocketFlow)(Keep.right)             //: ws.TextMessage                  (4)
      .mapAsync(1)(wsMessageToString)                //: String                          (5)
      .map("client received: " + _)                  //: String                          (6)
      .toMat(Sink.foreach(println))(Keep.both)       //                                  (7)
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

  finished.foreach(_ => println("stream finished"))

  for {
    _ <- actorSystem.terminate()
    _ <- WebServer.stop()
    _ <- ActiveMqBroker.stop()
  } ()

}
