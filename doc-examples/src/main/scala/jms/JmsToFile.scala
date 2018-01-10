/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package jms

// #sample
import java.nio.file.Paths

import akka.NotUsed
import akka.stream.IOResult
import akka.stream.alpakka.jms.JmsSourceSettings
import akka.stream.alpakka.jms.scaladsl.JmsSource
import akka.stream.scaladsl.{FileIO, Sink, Source}
import akka.util.ByteString
import playground.ActiveMqBroker

import scala.concurrent.Future
// #sample

object JmsToFile extends JmsSampleBase with App {

  ActiveMqBroker.start()

  val connectionFactory = ActiveMqBroker.createConnectionFactory
  enqueue(connectionFactory)("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k")

  // format: off
  // #sample

  val jmsSource: Source[String, _] =                 // (1)
    JmsSource.textSource(
      JmsSourceSettings(connectionFactory).withBufferSize(10).withQueue("test")
    )

  val fileSink: Sink[ByteString, Future[IOResult]] = // (2)
    FileIO.toPath(Paths.get("target/out.txt"))

  val finished: Future[IOResult] =                   // stream element type
    jmsSource                                        //: String
      .map(ByteString(_))                            //: ByteString    (3)
      .runWith(fileSink)
  // #sample
  // format: on
  wait(1)
  for {
    _ <- actorSystem.terminate()
    _ <- ActiveMqBroker.stop()
  } ()

}
