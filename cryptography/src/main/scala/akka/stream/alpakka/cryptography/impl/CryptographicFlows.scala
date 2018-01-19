/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.cryptography.impl

import java.security.{PrivateKey, PublicKey}
import javax.crypto.{Cipher, SecretKey}

import akka.NotUsed
import akka.stream.scaladsl.Flow
import akka.util.ByteString

object CryptographicFlows {

  def symmetricEncryption(secretKey: SecretKey): Flow[ByteString, ByteString, NotUsed] =
    Flow[ByteString].statefulMapConcat { () =>
      val cipher = Cipher.getInstance(secretKey.getAlgorithm)
      cipher.init(Cipher.ENCRYPT_MODE, secretKey)

      in =>
        {
          val encrypted = cipher.doFinal(in.toArray)

          List(ByteString(encrypted))
        }
    }

  def symmetricDecryption(secretKey: SecretKey): Flow[ByteString, ByteString, NotUsed] =
    Flow[ByteString].statefulMapConcat { () =>
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
        }

    }

  def asymmetricDecryption(privateKey: PrivateKey): Flow[ByteString, ByteString, NotUsed] =
    Flow[ByteString].statefulMapConcat { () =>
      val cipher = Cipher.getInstance(privateKey.getAlgorithm)
      cipher.init(Cipher.DECRYPT_MODE, privateKey)

      in =>
        {
          val decrypted = cipher.doFinal(in.toArray)

          List(ByteString(decrypted))
        }
    }
}
