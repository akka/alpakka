package docs.scaladsl

import java.io.FileNotFoundException
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.file.DirectoryChange
import akka.stream.alpakka.file.scaladsl.{DirectoryChangesSource, FileTailSource}
import akka.stream.scaladsl.{Keep, Source}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import com.google.common.jimfs.{Configuration, Jimfs}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.TimeoutException
import scala.concurrent.duration._

class FileTailSourceExtrasSpec
    extends TestKit(ActorSystem("filetailsourceextrasspec"))
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

      val checkInterval = 1.second
      val fileCheckSource = DirectoryChangesSource(path.getParent, checkInterval, 8192)
        .collect {
          case (p, DirectoryChange.Deletion) if path == p =>
            throw new FileNotFoundException(path.toString)
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

      // #shutdown-on-idle-timeout

      val stream = FileTailSource
        .lines(path = path, maxLineSize = 8192, pollingInterval = 250.millis)
        .idleTimeout(5.seconds)
        .recoverWithRetries(1, {
          case _: TimeoutException => Source.empty
        })

      // #shutdown-on-idle-timeout

      val probe = stream.toMat(TestSink.probe)(Keep.right).run()

      val result = probe.requestNext()
      result shouldEqual "a"

      Thread.sleep(5.seconds.toMillis + 1000)

      probe.expectComplete()
    }

  }
}
