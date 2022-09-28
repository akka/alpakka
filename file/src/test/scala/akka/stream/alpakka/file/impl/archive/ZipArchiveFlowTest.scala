/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.file.impl.archive

import akka.actor.ActorSystem
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import akka.stream.scaladsl.Keep
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.testkit.TestKit
import akka.util.ByteString
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpecLike

class ZipArchiveFlowTest
    extends TestKit(ActorSystem("ziparchive"))
    with AnyWordSpecLike
    with BeforeAndAfterAll
    with LogCapturing {

  "ZipArchiveFlowStage" when {
    "stream ends" should {
      "emit element only when downstream requests" in {
        val (upstream, downstream) =
          TestSource[ByteString]()
            .via(new ZipArchiveFlow())
            .toMat(TestSink())(Keep.both)
            .run()

        upstream.sendNext(FileByteStringSeparators.createStartingByteString("test"))
        upstream.sendNext(ByteString(1))
        upstream.sendNext(FileByteStringSeparators.createEndingByteString())
        upstream.sendComplete()

        downstream.request(2)
        downstream.expectNextN(2)
        downstream.request(1)
        downstream.expectNextN(1)
        downstream.expectComplete()
      }
    }
  }

  override def afterAll(): Unit = {
    super.afterAll()
    TestKit.shutdownActorSystem(system)
  }
}
