/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.s3.impl

import java.nio.BufferOverflowException
import java.nio.file.Files

import akka.actor.ActorSystem
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import akka.testkit.{EventFilter, TestKit}
import akka.util.ByteString
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

class DiskBufferSpec(_system: ActorSystem)
    extends TestKit(_system)
    with AnyFlatSpecLike
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures
    with Eventually
    with LogCapturing {

  def this() = this(ActorSystem("DiskBufferSpec"))

  implicit val defaultPatience =
    PatienceConfig(timeout = Span(2, Seconds), interval = Span(200, Millis))

  implicit val materializer = ActorMaterializer(ActorMaterializerSettings(system).withDebugLogging(true))

  override protected def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  "DiskBuffer" should
  "emit a chunk on its output containing the concatenation of all input values" in {
    val result = Source(Vector(ByteString(1, 2, 3, 4, 5), ByteString(6, 7, 8, 9, 10, 11, 12), ByteString(13, 14)))
      .via(new DiskBuffer(1, 200, None))
      .runWith(Sink.seq)
      .futureValue

    result should have size (1)
    val chunk = result.head
    chunk.size should be(14)
    chunk.data.runWith(Sink.seq).futureValue should be(Seq(ByteString(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14)))
  }

  it should "fail if more than maxSize bytes are fed into it" in {
    EventFilter[BufferOverflowException](occurrences = 1) intercept {
      whenReady(
        Source(Vector(ByteString(1, 2, 3, 4, 5), ByteString(6, 7, 8, 9, 10, 11, 12), ByteString(13, 14)))
          .via(new DiskBuffer(1, 10, None))
          .runWith(Sink.seq)
          .failed
      ) { e =>
        e shouldBe a[BufferOverflowException]
      }
    }
  }

  it should "delete its temp file after N materializations" in {
    val tmpDir = Files.createTempDirectory("DiskBufferSpec").toFile()
    val before = tmpDir.list().size
    val source = Source(Vector(ByteString(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14)))
      .via(new DiskBuffer(2, 200, Some(tmpDir.toPath)))
      .runWith(Sink.seq)
      .futureValue
      .head
      .data

    tmpDir.list().size should be(before + 1)

    source.runWith(Sink.ignore).futureValue
    tmpDir.list().size should be(before + 1)

    source.runWith(Sink.ignore).futureValue
    eventually {
      tmpDir.list().size should be(before)
    }

  }
}
