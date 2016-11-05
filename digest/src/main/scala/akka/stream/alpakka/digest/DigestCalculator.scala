/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.digest

import akka.stream._
import akka.stream.scaladsl.{ Flow, Keep, Sink }
import akka.stream.stage._
import akka.util.ByteString
import akka.{ Done, NotUsed }

import scala.concurrent.Future
import scala.util.Success

/**
 * The DigestCalculator transforms/digests a stream of akka.util.ByteString to a
 * DigestResult according to a given Algorithm
 */
object DigestCalculator {

  def flow(algorithm: Algorithm): Flow[ByteString, DigestResult, NotUsed] =
    Flow.fromGraph[ByteString, DigestResult, NotUsed](new DigestCalculator(algorithm))

  def sink(algorithm: Algorithm): Sink[ByteString, Future[DigestResult]] =
    flow(algorithm).toMat(Sink.head)(Keep.right)
}

private[digest] class DigestCalculator(algorithm: Algorithm) extends GraphStage[FlowShape[ByteString, DigestResult]] {
  val in: Inlet[ByteString] = Inlet("DigestCalculator.in")
  val out: Outlet[DigestResult] = Outlet("DigestCalculator.out")
  override val shape: FlowShape[ByteString, DigestResult] = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    val digest = java.security.MessageDigest.getInstance(algorithm.toString)

    def digestToHexString(messageDigest: Array[Byte]): String =
      messageDigest.map("%02x".format(_)).mkString

    setHandler(out, new OutHandler {
      override def onPull(): Unit = {
        pull(in)
      }
    })

    setHandler(in, new InHandler {
      override def onPush(): Unit = {
        val chunk = grab(in)
        digest.update(chunk.toArray)
        pull(in)
      }

      override def onUpstreamFinish(): Unit = {
        val messageDigest: Array[Byte] = digest.digest()
        emit(out, DigestResult(ByteString(messageDigest), digestToHexString(messageDigest), Success(Done)))
        completeStage()
      }
    })
  }
}
