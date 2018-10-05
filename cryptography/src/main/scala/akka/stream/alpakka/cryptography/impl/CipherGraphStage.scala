package akka.stream.alpakka.cryptography.impl
import java.io.ByteArrayOutputStream

import akka.stream.ActorAttributes.SupervisionStrategy
import akka.stream._
import akka.stream.impl.Stages.DefaultAttributes
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.util.ByteString
import javax.crypto.{Cipher, CipherOutputStream}

import scala.util.control.NonFatal

case class CipherGraphStage(cipher: Cipher) extends GraphStage[FlowShape[ByteString, ByteString]] {
  val in = Inlet[ByteString]("StatefulMapConcat.in")
  val out = Outlet[ByteString]("StatefulMapConcat.out")
  override val shape = FlowShape(in, out)

  override def initialAttributes: Attributes = DefaultAttributes.statefulMapConcat

  override def createLogic(inheritedAttributes: Attributes) =
    new GraphStageLogic(shape) with InHandler with OutHandler {
      lazy val decider =
        inheritedAttributes.get[SupervisionStrategy].map(_.decider).getOrElse(Supervision.stoppingDecider)

      var bufferSize = 16
      var outputStream = new ByteArrayOutputStream(bufferSize)
      var cipherOutputStream = new CipherOutputStream(outputStream, cipher)

      var currentIterator: Iterator[ByteString] = _
      var plainFun: ByteString => List[ByteString] = { bytes =>
        cipherOutputStream.write(bytes.toArray)
        cipherOutputStream.flush()
        val encryptedBytes = outputStream.toByteArray
        val byteString = ByteString(encryptedBytes)
        List(byteString)
      }
      def hasNext = if (currentIterator != null) currentIterator.hasNext else false

      setHandlers(in, out, this)

      def pushPull(): Unit =
        if (hasNext) {
          push(out, currentIterator.next())
          if (!hasNext && isClosed(in)) completeStage()
        } else if (!isClosed(in))
          pull(in)
        else completeStage()

      def onFinish(): Unit = if (!hasNext) {
        cipherOutputStream.close()
        completeStage()
      }

      override def onPush(): Unit =
        try {
          currentIterator = plainFun(grab(in)).iterator
          pushPull()
        } catch {
          case NonFatal(ex) ⇒
            decider(ex) match {
              case Supervision.Stop ⇒ failStage(ex)
              case Supervision.Resume ⇒ if (!hasBeenPulled(in)) pull(in)
              case Supervision.Restart ⇒
                restartState()
                if (!hasBeenPulled(in)) pull(in)
            }
        }

      def restartState() = {
        bufferSize = 16
        outputStream = new ByteArrayOutputStream(bufferSize)
        cipherOutputStream = new CipherOutputStream(outputStream, cipher)
      }

      override def onUpstreamFinish(): Unit = onFinish()

      override def onPull(): Unit = pushPull()
    }

  override def toString = "StatefulMapConcat"

}
