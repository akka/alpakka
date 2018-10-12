/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.cryptography.impl
import java.io.ByteArrayOutputStream

import akka.NotUsed
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.scaladsl.Flow
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.util.ByteString
import javax.crypto.{Cipher, CipherOutputStream}

object CipherFlow {
  def cipherFlow(cipher: Cipher): Flow[ByteString, ByteString, NotUsed] =
    Flow[ByteString]
//      .via(new ByteLimiter(16))
      .map { bytes =>
        val processed = new ByteArrayOutputStream(16)
        val cipherOutputStream = new CipherOutputStream(processed, cipher)
        cipherOutputStream.write(bytes.toArray)
        cipherOutputStream.close()
        ByteString(processed.toByteArray)
      }
}

class ByteLimiter(val maximumBytes: Long) extends GraphStage[FlowShape[ByteString, ByteString]] {
  val in = Inlet[ByteString]("ByteLimiter.in")
  val out = Outlet[ByteString]("ByteLimiter.out")
  override val shape = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private var count = 0

    setHandlers(
      in,
      out,
      new InHandler with OutHandler {

        override def onPull(): Unit =
          pull(in)

        override def onPush(): Unit = {
          val chunk = grab(in)
          count += chunk.size
          if (count > maximumBytes) failStage(new IllegalStateException("Too much bytes"))
          else push(out, chunk)
        }
      }
    )
  }
}
