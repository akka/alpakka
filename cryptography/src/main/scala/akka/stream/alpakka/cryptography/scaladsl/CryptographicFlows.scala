/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.cryptography.scaladsl

import akka.NotUsed
import akka.stream.alpakka.cryptography.impl.CipherFlow
import akka.stream.scaladsl.Flow
import akka.util.ByteString
import javax.crypto.Cipher

object CryptographicFlows {
  def cipherFlow(cipher: Cipher): Flow[ByteString, ByteString, NotUsed] =
    CipherFlow.cipherFlow(cipher)
}
