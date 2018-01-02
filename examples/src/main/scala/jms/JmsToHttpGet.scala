/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package jms

// #sample
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.alpakka.jms.JmsSourceSettings
import akka.stream.alpakka.jms.scaladsl.JmsSource
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import akka.{Done, NotUsed}
import playground.{ActiveMqBroker, WebServer}

import scala.concurrent.Future

// #sample

object JmsToHttpGet extends JmsSampleBase with App {

  WebServer.start()
  ActiveMqBroker.start()

  val connectionFactory = ActiveMqBroker.createConnectionFactory
  enqueue(connectionFactory)("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k")

  // format: off
  // #sample
  val jmsSource: Source[String, NotUsed] =                                // (1)
    JmsSource.textSource(
      JmsSourceSettings(connectionFactory).withBufferSize(10).withQueue("test")
    )

  val finished: Future[Done] =
    jmsSource
      .map(ByteString(_))                                                   // (2)
      .map { bs =>
        HttpRequest(uri = Uri("http://localhost:8080/hello"),               // (3)
          entity = HttpEntity(bs))
      }
      .mapAsyncUnordered(4)(Http().singleRequest(_))                        // (4)
      .runWith(Sink.foreach(println))                                       // (5)
  // #sample
  // format: on
  finished.foreach(_ => println("stream finished"))

  wait(10)
  for {
    _ <- actorSystem.terminate()
    _ <- WebServer.stop()
    _ <- ActiveMqBroker.stop()
  } ()

}
