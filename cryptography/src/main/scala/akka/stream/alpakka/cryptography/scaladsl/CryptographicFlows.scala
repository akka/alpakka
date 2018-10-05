/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.cryptography.scaladsl

import akka.NotUsed
import akka.stream.scaladsl.Flow
import akka.util.ByteString
import akka.stream.alpakka.cryptography.impl.CipherGraphStage

import javax.crypto.Cipher

object CryptographicFlows {
  def cipherFlow(cipher: Cipher): Flow[ByteString, ByteString, NotUsed] =
    Flow.fromGraph(CipherGraphStage(cipher))
}
