/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.file.scaladsl

import java.nio.file.{Files, Path}

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.TestKit
import com.google.common.jimfs.{Configuration, Jimfs}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

class DirectorySpec
    extends TestKit(ActorSystem("directoryspec"))
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures {

  private val fs = Jimfs.newFileSystem(Configuration.forCurrentPlatform.toBuilder.build)
  private implicit val mat = ActorMaterializer()

  "The directory source factory" should {
    "list files" in {
      val dir = fs.getPath("listfiles")
      Files.createDirectories(dir)
      val paths = (0 to 100).map { n =>
        val name = s"file$n"
        Files.createFile(dir.resolve(name))
      }

      // #ls
      val source: Source[Path, NotUsed] = Directory.ls(dir)
      // #ls

      val result = source.runWith(Sink.seq).futureValue
      result.toSet shouldEqual paths.toSet
    }

    "walk a file tree" in {
      val root = fs.getPath("walk")
      Files.createDirectories(root)
      val subdir1 = root.resolve("subdir1")
      Files.createDirectories(subdir1)
      val file1 = subdir1.resolve("file1")
      Files.createFile(file1)
      val subdir2 = root.resolve("subdir2")
      Files.createDirectories(subdir2)
      val file2 = subdir2.resolve("file2")
      Files.createFile(file2)

      // #walk
      val files: Source[Path, NotUsed] = Directory.walk(root)
      // #walk

      val result = files.runWith(Sink.seq).futureValue
      result shouldEqual List(root, subdir1, file1, subdir2, file2)
    }
  }

  override protected def afterAll(): Unit =
    fs.close()
}
