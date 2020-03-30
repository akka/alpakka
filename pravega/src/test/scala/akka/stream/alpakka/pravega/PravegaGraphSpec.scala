/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.pravega

import akka.stream.KillSwitches
import akka.stream.alpakka.pravega.scaladsl.Pravega

import akka.stream.scaladsl.{Keep, Sink, Source}
import io.pravega.client.stream.impl.UTF8StringSerializer

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Promise}

class PravegaGraphSpec extends PravegaBaseSpec {

  val serializer = new UTF8StringSerializer

  implicit val readerSettings = ReaderSettingsBuilder(system)
    .withSerializer(serializer)

  implicit val writerSettings = WriterSettingsBuilder(system)
    .withSerializer(serializer)

  val writerSettingsWithRoutingKey = WriterSettingsBuilder(system)
    .withKeyExtractor((str: String) => str.substring(0, 2))
    .withSerializer(serializer)

  val nEvent = 1000

  "Pravega connector" should {

    "runs sources" in {

      logger.info("start source")

      // #writing
      Source(1 to nEvent)
        .map(i => f"$i%02d_event")
        .runWith(Pravega.sink(scope, streamName))

      Source(1 to nEvent)
        .map(i => f"$i%02d_event")
        .runWith(Pravega.sink(scope, streamName)(writerSettingsWithRoutingKey))

      // #writing

      val finishReading = Promise[Unit]

      // #reading

      val (kill, fut) = Pravega
        .source(scope, streamName)
        .viaMat(KillSwitches.single)(Keep.right)
        .toMat(Sink.fold(nEvent * 2) { (acc, _) =>
          if (acc == 1)
            finishReading.success(())
          acc - 1
        })(Keep.both)
        .run()

      // #reading

      Await.ready(finishReading.future, 20.seconds)

      logger.debug("Die, die by my hand.")
      kill.shutdown()

      whenReady(fut) { r =>
        r mustEqual 0
        logger.info(s"Read $nEvent events.")
      }
    }

  }

}
