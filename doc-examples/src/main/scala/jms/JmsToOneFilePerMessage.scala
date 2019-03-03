/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package jms

// #sample
import java.nio.file.Paths

import akka.stream.alpakka.jms.JmsConsumerSettings
import akka.stream.alpakka.jms.scaladsl.{JmsConsumer, JmsConsumerControl}
import akka.stream.scaladsl.{FileIO, Keep, Sink, Source}
import akka.util.ByteString

import scala.concurrent.duration.DurationInt
// #sample
import playground.ActiveMqBroker

object JmsToOneFilePerMessage extends JmsSampleBase with App {

  ActiveMqBroker.start()

  val connectionFactory = ActiveMqBroker.createConnectionFactory
  enqueue(connectionFactory)("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k")

  // format: off
  // #sample

  val jmsSource: Source[String, JmsConsumerControl] =                                   // (1)
    JmsConsumer.textSource(
      JmsConsumerSettings(actorSystem, connectionFactory).withBufferSize(10).withQueue("test")
    )
                                                            // stream element type
  val runningSource = jmsSource                             //: String
    .map(ByteString(_))                                     //: ByteString         (2)
    .zipWithIndex                                           //: (ByteString, Long) (3)
    .mapAsyncUnordered(parallelism = 5) { case (byteStr, number) =>
      Source                                                //                     (4)
        .single(byteStr)
        .runWith(FileIO.toPath(Paths.get(s"target/out-$number.txt")))
    }                                                       //: IoResult
    .toMat(Sink.ignore)(Keep.left)
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
