package docs.scaladsl

import java.io.FileNotFoundException
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}
import java.util.Collections

import akka.actor.{ActorSystem, Cancellable}
import akka.stream.ActorMaterializer
import akka.stream.alpakka.file.scaladsl.FileTailSource
import akka.stream.scaladsl.{Keep, Source}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import com.google.common.jimfs.{Configuration, Jimfs}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.TimeoutException
import scala.concurrent.duration._

class FileTailSourceExtrasSpec extends TestKit(ActorSystem("filetailsourceextrasspec"))
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll
  with ScalaFutures {

  private val fs = Jimfs.newFileSystem(Configuration.forCurrentPlatform.toBuilder.build)
  private implicit val mat = ActorMaterializer()

  "The FileTailSource" should assertAllStagesStopped {
    "demo stream shutdown when file deleted" in {
      val path = fs.getPath("/file")
      Files.write(path, "a\n".getBytes(UTF_8))

      // #shutdown-on-delete

      final case object Tick

      val checkInterval = 1.second

      val fileCheckSource = Source
        .tick(checkInterval, checkInterval, Tick)
        .mapConcat { _ =>
          if (Files.exists(path))
            Nil
          else throw new FileNotFoundException
        }
        .recoverWithRetries(1, {
          case _: FileNotFoundException => Source.empty
        })

      val stream = FileTailSource
        .lines(path = path, maxLineSize = 8192, pollingInterval = 250.millis)
        .merge(fileCheckSource, eagerComplete = true)

      // #shutdown-on-delete

      val probe = stream.toMat(TestSink.probe)(Keep.right).run()

      val result = probe.requestNext()
      result shouldEqual "a"

      Files.delete(path)

      probe.request(1)
      probe.expectComplete()
    }

    "demo stream shutdown when with idle timeout" in {
      val path = fs.getPath("/file")
      Files.write(path, "a\n".getBytes(UTF_8))

      // just for docs
      // #shutdown-on-idle-timeout

      val stream = FileTailSource
        .lines(path = path, maxLineSize = 8192, pollingInterval = 250.millis)
        .idleTimeout(30.seconds)
        .recoverWithRetries(1, {
          case _: TimeoutException => Source.empty
        })

      // #shutdown-on-idle-timeout

      val idleTimeout2 = 1.seconds

      val actualStream = FileTailSource
        .lines(path = path, maxLineSize = 8192, pollingInterval = 250.millis)
        .idleTimeout(idleTimeout2)
        .recoverWithRetries(1, {
          case _: TimeoutException => Source.empty
        })

      val probe = actualStream.toMat(TestSink.probe)(Keep.right).run()

      val result = probe.requestNext()
      result shouldEqual "a"

      Thread.sleep(idleTimeout2.toMillis + 1000)

      probe.expectComplete()
    }

  }
}
