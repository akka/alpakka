/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.pravega

import akka.stream.alpakka.pravega.scaladsl.Pravega

import io.pravega.client.stream.impl.UTF8StringSerializer

import akka.stream.scaladsl.Sink
import akka.stream.KillSwitches
import akka.stream.scaladsl.Keep
import scala.concurrent.Await

import scala.concurrent.Promise
import scala.concurrent.duration.Duration
import akka.stream.alpakka.testkit.scaladsl.Repeated
import akka.stream.scaladsl.Source

class PravegaReaderWithTimeoutSpec extends PravegaBaseSpec with Repeated {

  "Pravega source" must {
    "handle timeout while reading" in {

      val scope = newScope()
      val stream = newStreamName()

      createStream(scope, stream)

      implicit val writerSettings = WriterSettingsBuilder(system)
        .withSerializer(new UTF8StringSerializer)

      implicit val readerSettings = ReaderSettingsBuilder(system)
        .withGroupName("meetup-lib-15")
        .withSerializer(new UTF8StringSerializer)

      val eventToread = 20
      val finishReading = Promise[Unit]()

      logger.info("Start ...")

      val (kill, fut) = Pravega
        .source(scope, stream)
        .viaMat(KillSwitches.single)(Keep.right)
        .toMat(Sink.fold(eventToread) { (acc, _) =>
          if (acc == 1)
            finishReading.success(())
          acc - 1
        })(Keep.both)
        .run()

      logger.info("Running ...")

      val sink = Pravega.sink(scope, stream)

      Source(1 to 10)
        .map(i => f"name_$i%d")
        .runWith(sink)

      Thread.sleep(3000)

      Source(1 to 10)
        .map(i => f"name_$i%d")
        .runWith(sink)

      Await.ready(finishReading.future, Duration.Inf)

      logger.info(s"Read at least $eventToread...")

      kill.shutdown()

      whenReady(fut) { r =>
        r mustEqual 0
        logger.info(s"Read $eventToread events.")
      }

    }

  }
}
