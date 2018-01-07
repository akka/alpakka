/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package jms

// #sample
import java.nio.file.Paths

import akka.NotUsed
import akka.stream.alpakka.jms.JmsSourceSettings
import akka.stream.alpakka.jms.scaladsl.JmsSource
import akka.stream.scaladsl.{FileIO, Sink, Source}
import akka.util.ByteString
import playground.ActiveMqBroker
// #sample

object JmsToOneFilePerMessage extends JmsSampleBase with App {

  ActiveMqBroker.start()

  val connectionFactory = ActiveMqBroker.createConnectionFactory
  enqueue(connectionFactory)("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k")

  // format: off
  // #sample

  val jmsSource: Source[String, _] =                        // (1)
    JmsSource.textSource(
      JmsSourceSettings(connectionFactory).withBufferSize(10).withQueue("test")
    )
                                                            // stream element type
  jmsSource                                                 //: String
    .map(ByteString(_))                                     //: ByteString        (2)
    .zip(Source.fromIterator(() => Iterator.from(0)))       //: (ByteString, Int) (3)
    .mapAsyncUnordered(parallelism = 5) { case (byteStr, number) =>
      Source                                                //                    (4)
        .single(byteStr)
        .runWith(FileIO.toPath(Paths.get(s"target/out-$number.txt")))
    }                                                       //: IoResult
    .runWith(Sink.ignore)
  // #sample
  // format: on
  wait(1)
  for {
    _ <- actorSystem.terminate()
    _ <- ActiveMqBroker.stop()
  } ()

}
