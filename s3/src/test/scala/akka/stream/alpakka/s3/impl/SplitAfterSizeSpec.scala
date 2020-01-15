/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.s3.impl

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.testkit.TestKit
import akka.util.ByteString
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import scala.concurrent.duration._
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

class SplitAfterSizeSpec(_system: ActorSystem)
    extends TestKit(_system)
    with AnyFlatSpecLike
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures {

  def this() = this(ActorSystem("SplitAfterSizeSpec"))
  implicit val defaultPatience: PatienceConfig = PatienceConfig(1.seconds, 5.millis)

  implicit val materializer = ActorMaterializer(ActorMaterializerSettings(system).withDebugLogging(true))

  override protected def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  final val MaxChunkSize = 1024
  "SplitAfterSize" should "yield a single empty substream on no input" in assertAllStagesStopped {
    Source
      .empty[ByteString]
      .via(
        SplitAfterSize(10, MaxChunkSize)(Flow[ByteString]).concatSubstreams
      )
      .runWith(Sink.seq)
      .futureValue should be(Seq.empty)
  }

  it should "start a new stream after the element that makes it reach a maximum, but not split the element itself" in assertAllStagesStopped {
    Source(Vector(ByteString(1, 2, 3, 4, 5), ByteString(6, 7, 8, 9, 10, 11, 12), ByteString(13, 14)))
      .via(
        SplitAfterSize(10, MaxChunkSize)(Flow[ByteString])
          .prefixAndTail(10)
          .map { case (prefix, tail) => prefix }
          .concatSubstreams
      )
      .runWith(Sink.seq)
      .futureValue should be(
      Seq(
        Seq(ByteString(1, 2, 3, 4, 5), ByteString(6, 7, 8, 9, 10, 11, 12)),
        Seq(ByteString(13, 14))
      )
    )
  }

  it should "split large elements" in assertAllStagesStopped {
    Source(Vector(ByteString(bytes(1, 16)), ByteString(17, 18)))
      .via(
        SplitAfterSize(10, maxChunkSize = 15)(Flow[ByteString])
          .prefixAndTail(10)
          .map { case (prefix, tail) => prefix }
          .concatSubstreams
      )
      .runWith(Sink.seq)
      .futureValue should be(
      Seq(
        Seq(ByteString(bytes(1, 15))),
        Seq(ByteString(16), ByteString(17, 18))
      )
    )
  }

  it should "split large elements following a smaller element" in assertAllStagesStopped {
    Source(Vector(ByteString(101, 102), ByteString(bytes(1, 16)), ByteString(17, 18)))
      .via(
        SplitAfterSize(10, maxChunkSize = 15)(Flow[ByteString])
          .prefixAndTail(10)
          .map { case (prefix, tail) => prefix }
          .concatSubstreams
      )
      .runWith(Sink.seq)
      .futureValue should be(
      Seq(
        Seq(ByteString(101, 102), ByteString(bytes(1, 13))),
        Seq(ByteString(14, 15, 16), ByteString(17, 18))
      )
    )
  }

  it should "split large elements multiple times" in assertAllStagesStopped {
    Source(Vector(ByteString(bytes(1, 32)), ByteString(1, 2)))
      .via(
        SplitAfterSize(10, maxChunkSize = 15)(Flow[ByteString])
          .prefixAndTail(10)
          .map { case (prefix, tail) => prefix }
          .concatSubstreams
      )
      .runWith(Sink.seq)
      .futureValue should be(
      Seq(
        Seq(ByteString(bytes(1, 15))),
        Seq(ByteString(bytes(16, 30))),
        Seq(ByteString(31, 32), ByteString(1, 2))
      )
    )
  }

  it should "split large elements that would overflow the max chunk size" in assertAllStagesStopped {
    Source(Vector(ByteString(1, 2, 3, 4), ByteString(bytes(5, 16)), ByteString(17, 18)))
      .via(
        SplitAfterSize(10, maxChunkSize = 15)(Flow[ByteString])
          .prefixAndTail(10)
          .map { case (prefix, tail) => prefix }
          .concatSubstreams
      )
      .runWith(Sink.seq)
      .futureValue should be(
      Seq(
        Seq(ByteString(1, 2, 3, 4), ByteString(5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15)),
        Seq(ByteString(16), ByteString(17, 18))
      )
    )
  }

  def bytes(start: Byte, end: Byte): Array[Byte] = (start to end).map(_.toByte).toArray[Byte]

}
