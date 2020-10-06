/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.pravega

import akka.stream.KillSwitches

import akka.stream.alpakka.pravega.scaladsl.Pravega
import akka.stream.alpakka.testkit.scaladsl.Repeated
import akka.stream.scaladsl.{Keep, Sink, Source}
import com.typesafe.config.ConfigFactory
import io.pravega.client.stream.impl.UTF8StringSerializer

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Promise}
import scala.util.Using

class PravegaGraphSpec extends PravegaBaseSpec with Repeated {

  val serializer = new UTF8StringSerializer
  val nEvent = 1000
  val timeout = 10.seconds

  "Pravega connector" should {

    "runs sources" in {

      val group = newGroupName()
      val scope = newScope()

      val stream1 = "scala-test-stream1"
      val stream2 = "scala-test-stream2"

      createStream(scope, stream1)
      createStream(scope, stream2)

      implicit val readerSettings = ReaderSettingsBuilder(
        system.settings.config
          .getConfig(ReaderSettingsBuilder.configPath)
          .withFallback(ConfigFactory.parseString(s"group-name = ${newGroupName()}"))
      ).withSerializer(serializer)

      implicit val writerSettings = WriterSettingsBuilder(system)
        .withSerializer(serializer)

      val writerSettingsWithRoutingKey = WriterSettingsBuilder(system)
        .withKeyExtractor((str: String) => str.substring(0, 2))
        .withSerializer(serializer)

      logger.info(s"Write $nEvent events")

      // #writing
      val done = time(s"Write $nEvent events",
                      Source(1 to nEvent)
                        .map(i => f"$i%02d_event")
                        .runWith(Pravega.sink(scope, stream1)))

      time("Wait write", Await.ready(done, timeout))

      Source(1 to nEvent)
        .map(i => f"$i%02d_event")
        .runWith(Pravega.sink(scope, stream2))

      Thread.sleep(3000)

      Source(1 to nEvent)
        .map(i => f"$i%02d_event")
        .runWith(Pravega.sink(scope, stream1)(writerSettingsWithRoutingKey))

      // #writing

      val finishReading = Promise[Unit]()

      // #reading

      Using(Pravega.readerGroupManager(scope, readerSettings.clientConfig)) { readerGroupManager =>
        readerGroupManager.createReaderGroup(group, stream1, stream2)
      }.foreach { readerGroup =>
        val (kill, fut) = Pravega
          .source(readerGroup)
          .viaMat(KillSwitches.single)(Keep.right)
          .toMat(Sink.fold(nEvent * 3) { (acc, _) =>
            if (acc == 1)
              finishReading.success(())
            acc - 1
          })(Keep.both)
          .run()

        // #reading

        Await.ready(finishReading.future, timeout)

        logger.debug("Die, die by my hand.")
        kill.shutdown()

        whenReady(fut) { r =>
          r mustEqual 0
          logger.info(s"Read $nEvent events.")
        }
      }

    }

  }

}
