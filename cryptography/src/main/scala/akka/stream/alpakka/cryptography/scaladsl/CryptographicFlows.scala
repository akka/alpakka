/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.cryptography.scaladsl

import java.security.{PrivateKey, PublicKey}
import javax.crypto.{Cipher, SecretKey}

import akka.NotUsed
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.scaladsl.Flow
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.util.ByteString


object CryptographicFlows {

  def symmetricEncryption(secretKey: SecretKey): Flow[ByteString, ByteString, NotUsed] =
    Flow
      .fromGraph(chunk(1028))
      .statefulMapConcat { () =>
        val cipher = Cipher.getInstance(secretKey.getAlgorithm)
        cipher.init(Cipher.ENCRYPT_MODE, secretKey)

        in =>
          {
            val encrypted = cipher.doFinal(in.toArray)

            List(ByteString(encrypted))
          }
      }

  def symmetricDecryption(secretKey: SecretKey): Flow[ByteString, ByteString, NotUsed] =
    Flow
      .fromGraph(chunk(1028))
      .statefulMapConcat { () =>
        val cipher = Cipher.getInstance(secretKey.getAlgorithm)
        cipher.init(Cipher.DECRYPT_MODE, secretKey)

        in =>
          {
            val decrypted = cipher.doFinal(in.toArray)

            List(ByteString(decrypted))
          }
      }

  def asymmetricEncryption(publicKey: PublicKey): Flow[ByteString, ByteString, NotUsed] =
    Flow[ByteString].statefulMapConcat { () =>
      val cipher = Cipher.getInstance(publicKey.getAlgorithm)
      cipher.init(Cipher.PUBLIC_KEY, publicKey)
      in =>
        {
          val encrypted = cipher.doFinal(in.toArray)

          List(ByteString(encrypted))
  def chunk(size: Int): Flow[ByteString, ByteString, NotUsed] =
    Flow[ByteString]
      .mapConcat(_.toList)
      .grouped(size)
      .map(bytes => ByteString(bytes: _*))

  def alternativeChunk(size: Int): Flow[ByteString, ByteString, NotUsed] =
    Flow.fromGraph(new GraphStage[FlowShape[ByteString, ByteString]] {

      val in: Inlet[ByteString] = Inlet("Chunk.in")
      val out: Outlet[ByteString] = Outlet("Chunk.out")

      override def shape: FlowShape[ByteString, ByteString] = FlowShape(in, out)

      override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) with InHandler with OutHandler {

        var rest = ByteString.empty

        override def onPush(): Unit = {
          receive()
          if(!hasBeenPulled(in)) pull(in)
        }

        override def onPull(): Unit = {
          while(!hasBeenPulled(in)) {
            pull(in)
            receive()
          }
        }

        def receive(): Unit = {
          while (isAvailable(in)) {
            val newlyRead = grab(in)
            val chunks = rest.concat(newlyRead)

            chunks.grouped(size).toList match {
              case Nil =>
              case fullChunks :+ last =>
                fullChunks.foreach(emit(out, _))

                if (last.length == size) {
                  emit(out, last)
                  rest = ByteString.empty
                } else {
                  rest = last
                }
            }
          }
        }


        override def onUpstreamFinish(): Unit = {
          if(rest.nonEmpty) {
            emit(out, rest)
            rest = ByteString.empty
          }

          completeStage()
        }

        setHandlers(in, out, this)
      }

    })
}
