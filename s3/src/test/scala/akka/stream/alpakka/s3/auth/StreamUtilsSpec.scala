/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.s3.auth

import java.nio.charset.StandardCharsets._
import java.nio.file.{Files, Path}
import java.security.DigestInputStream
import java.security.MessageDigest

import scala.concurrent.Future

import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.Millis
import org.scalatest.time.Seconds
import org.scalatest.time.Span

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.ActorMaterializerSettings
import akka.stream.scaladsl.Source
import akka.stream.scaladsl.StreamConverters
import akka.testkit.TestKit
import akka.util.ByteString
import com.google.common.jimfs.{Configuration, Jimfs}

class StreamUtilsSpec(_system: ActorSystem)
    extends TestKit(_system)
    with FlatSpecLike
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll {
  def this() = this(ActorSystem("StreamUtilsSpec"))

  implicit val materializer = ActorMaterializer(ActorMaterializerSettings(system).withDebugLogging(true))

  implicit val defaultPatience =
    PatienceConfig(timeout = Span(5, Seconds), interval = Span(30, Millis))

  val fs = Jimfs.newFileSystem("FileSourceSpec", Configuration.unix())

  val TestText = {
    ("a" * 1000) +
    ("b" * 1000) +
    ("c" * 1000) +
    ("d" * 1000) +
    ("e" * 1000) +
    ("f" * 1000)
  }

  val bigFile: Path = {
    val f = Files.createTempFile(fs.getPath("/"), "file-source-spec", ".tmp")
    val writer = Files.newBufferedWriter(f, UTF_8)
    (1 to 3500).foreach(_ => writer.append(TestText))
    writer.close()
    f
  }

  "digest" should "calculate the digest of a short string" in {
    val bytes = "abcdefghijklmnopqrstuvwxyz".getBytes()
    val flow = Source.single(ByteString(bytes)).runWith(digest())

    val testDigest = MessageDigest.getInstance("SHA-256").digest(bytes)
    whenReady(flow) { result =>
      result should contain theSameElementsInOrderAs testDigest
    }
  }

  it should "calculate the digest of a file" in {
    val input = StreamConverters.fromInputStream(() => Files.newInputStream(bigFile))
    val flow = input.runWith(digest())

    val testDigest = MessageDigest.getInstance("SHA-256")
    val dis = new DigestInputStream(Files.newInputStream(bigFile), testDigest)

    val buffer = new Array[Byte](1024)

    var bytesRead: Int = dis.read(buffer)
    while (bytesRead > -1) {
      bytesRead = dis.read(buffer)
    }

    whenReady(flow) { result =>
      result should contain theSameElementsInOrderAs dis.getMessageDigest.digest()
    }
  }

  override def afterAll(): Unit = fs.close()

}
