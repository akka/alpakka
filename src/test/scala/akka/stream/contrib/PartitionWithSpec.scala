/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.contrib

import akka.stream.scaladsl._
import akka.stream.{ FlowShape }
import akka.stream.testkit.scaladsl.{ TestSink, TestSource }

class PartitionWithSpecAutoFusingOn extends { val autoFusing = true } with PartitionWithSpec
class PartitionWithSpecAutoFusingOff extends { val autoFusing = false } with PartitionWithSpec

trait PartitionWithSpec extends BaseStreamSpec {

  val flow = Flow.fromGraph(GraphDSL.create() { implicit b =>

    import GraphDSL.Implicits._

    val pw = b.add(PartitionWith[Int, Int, Int] {
      case i if i % 2 == 0 => Left(i / 2)
      case i               => Right(i * 3 - 1)
    })

    val mrg = b.add(Merge[Int](2))

    pw.out0 ~> mrg.in(0)
    pw.out1 ~> mrg.in(1)

    FlowShape(pw.in, mrg.out)
  })

  "PartitionWith" should {
    "partition ints according to their parity" in {

      val (source, sink) = TestSource.probe[Int]
        .via(flow)
        .toMat(TestSink.probe)(Keep.both)
        .run()
      sink.request(99)
      source.sendNext(1)
      source.sendNext(2)
      source.sendNext(3)
      sink.expectNext(2, 1, 8)
      source.sendComplete()
      sink.expectComplete()
    }

    "not emit any value for an empty source" in {
      Source(Vector.empty[Int])
        .via(flow)
        .runWith(TestSink.probe)
        .request(99)
        .expectComplete()
    }

    "fail on upstream failure" in {
      val (source, sink) = TestSource.probe[Int]
        .via(flow)
        .toMat(TestSink.probe)(Keep.both)
        .run()
      sink.request(99)
      source.sendError(new Exception)
      sink.expectError()
    }
  }
}
