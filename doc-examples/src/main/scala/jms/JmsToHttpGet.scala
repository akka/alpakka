/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package jms

// #sample
import akka.Done
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.alpakka.jms.JmsConsumerSettings
import akka.stream.alpakka.jms.scaladsl.{JmsConsumer, JmsConsumerControl}
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.util.ByteString

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

// #sample
import playground.{ActiveMqBroker, WebServer}

object JmsToHttpGet extends JmsSampleBase with App {

  WebServer.start()
  ActiveMqBroker.start()

  val connectionFactory = ActiveMqBroker.createConnectionFactory
  enqueue(connectionFactory)("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k")

  // format: off
  // #sample
  val jmsSource: Source[String, JmsConsumerControl] =                                 // (1)
    JmsConsumer.textSource(
      JmsConsumerSettings(actorSystem,connectionFactory).withBufferSize(10).withQueue("test")
    )

  val (runningSource, finished): (JmsConsumerControl, Future[Done]) =
    jmsSource                                                   //: String
      .map(ByteString(_))                                       //: ByteString   (2)
      .map { bs =>
        HttpRequest(uri = Uri("http://localhost:8080/hello"),   //: HttpRequest  (3)
          entity = HttpEntity(bs))
      }
      .mapAsyncUnordered(4)(Http().singleRequest(_))            //: HttpResponse (4)
      .toMat(Sink.foreach(println))(Keep.both)                  //               (5)
      .run()
  // #sample
  // format: on
  finished.foreach(_ => println("stream finished"))

  wait(5.seconds)
  runningSource.shutdown()
  for {
    _ <- actorSystem.terminate()
    _ <- WebServer.stop()
    _ <- ActiveMqBroker.stop()
  } ()

}
