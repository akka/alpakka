/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package jms

// #sample
import java.nio.file.Paths

import akka.stream.IOResult
import akka.stream.alpakka.jms.JmsConsumerSettings
import akka.stream.alpakka.jms.scaladsl.{JmsConsumer, JmsConsumerControl}
import akka.stream.scaladsl.{FileIO, Keep, Sink, Source}
import akka.util.ByteString

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
// #sample
import playground.ActiveMqBroker

object JmsToFile extends JmsSampleBase with App {

  ActiveMqBroker.start()

  val connectionFactory = ActiveMqBroker.createConnectionFactory
  enqueue(connectionFactory)("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k")

  // format: off
  // #sample

  val jmsSource: Source[String, JmsConsumerControl] =        // (1)
    JmsConsumer.textSource(
      JmsConsumerSettings(actorSystem, connectionFactory).withBufferSize(10).withQueue("test")
    )

  val fileSink: Sink[ByteString, Future[IOResult]] = // (2)
    FileIO.toPath(Paths.get("target/out.txt"))

  val (runningSource, finished): (JmsConsumerControl, Future[IOResult]) =
                                                     // stream element type
    jmsSource                                        //: String
      .map(ByteString(_))                            //: ByteString    (3)
      .toMat(fileSink)(Keep.both)
      .run()
  // #sample
  // format: on
  wait(1.second)
  runningSource.shutdown()
  for {
    _ <- actorSystem.terminate()
    _ <- ActiveMqBroker.stop()
  } ()

}
