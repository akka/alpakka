/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.s3.impl

import akka.actor.ActorSystem
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.TestKit
import akka.util.ByteString
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}

class MemoryBufferSpec(_system: ActorSystem)
    extends TestKit(_system)
    with AnyFlatSpecLike
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures
    with LogCapturing {

  def this() = this(ActorSystem("MemoryBufferSpec"))

  implicit val defaultPatience: PatienceConfig =
    PatienceConfig(timeout = Span(5, Seconds), interval = Span(30, Millis))

  override protected def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  "MemoryBuffer" should "emit a chunk on its output containg the concatenation of all input values" in {
    val result = Source(Vector(ByteString(1, 2, 3, 4, 5), ByteString(6, 7, 8, 9, 10, 11, 12), ByteString(13, 14)))
      .via(new MemoryBuffer(200))
      .runWith(Sink.seq)
      .futureValue

    result should have size 1
    val chunk = result.head
    chunk.size should be(14)

    chunk shouldBe a[MemoryChunk]
    chunk.asInstanceOf[MemoryChunk].data should be(ByteString(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14))
  }

  it should "fail if more than maxSize bytes are fed into it" in {
    whenReady(
      Source(Vector(ByteString(1, 2, 3, 4, 5), ByteString(6, 7, 8, 9, 10, 11, 12), ByteString(13, 14)))
        .via(new MemoryBuffer(10))
        .runWith(Sink.seq)
        .failed
    ) { e =>
      e shouldBe a[IllegalStateException]
    }
  }
}
