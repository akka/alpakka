/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.storage.impl
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.Await
import scala.concurrent.duration._

class ChunkerSpec extends AnyWordSpec with ScalaFutures with Matchers with BeforeAndAfterAll {
  implicit val system = ActorSystem("ChunkerSpec")
  implicit val mat = ActorMaterializer()

  override def afterAll(): Unit =
    Await.result(system.terminate(), 5.seconds)

  "Chunker" should {
    "yield empty flow on no input" in {
      Source
        .empty[ByteString]
        .via(new Chunker(10))
        .runWith(Sink.seq)
        .futureValue shouldBe Seq.empty
    }

    "emit elements of the same size except last one (size must be < chunkSize) when totalSize % chunkSize != 0" in {
      Source(List(ByteString(1, 2), ByteString(3, 4, 1, 2), ByteString(3, 1, 2)))
        .via(new Chunker(2))
        .runWith(Sink.seq)
        .futureValue shouldBe Seq(Chunk(ByteString(1, 2)),
                                  Chunk(ByteString(3, 4)),
                                  Chunk(ByteString(1, 2)),
                                  Chunk(ByteString(3, 1)),
                                  Chunk(ByteString(2), Some(9)))
    }

    "emit elements of the same size when totalSize % chunkSize == 0" in {
      Source(List(ByteString(1, 2, 3, 4), ByteString(1, 2, 3), ByteString(1, 2, 3)))
        .via(new Chunker(2))
        .runWith(Sink.seq)
        .futureValue shouldBe Seq(Chunk(ByteString(1, 2)),
                                  Chunk(ByteString(3, 4)),
                                  Chunk(ByteString(1, 2)),
                                  Chunk(ByteString(3, 1)),
                                  Chunk(ByteString(2, 3), Some(10)))
    }
  }
}
