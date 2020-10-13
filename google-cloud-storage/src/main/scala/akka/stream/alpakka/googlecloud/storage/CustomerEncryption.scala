/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.storage

final class CustomerEncryption private (encryptionAlgorithm: String, keySha256: String) {
  def withEncryptionAlgorithm(encryptionAlgorithm: String): CustomerEncryption =
    copy(encryptionAlgorithm = encryptionAlgorithm)
  def withKeySha256(keySha256: String): CustomerEncryption = copy(keySha256 = keySha256)

  private def copy(encryptionAlgorithm: String = encryptionAlgorithm,
                   keySha256: String = keySha256
  ): CustomerEncryption =
    new CustomerEncryption(encryptionAlgorithm, keySha256)

  override def toString: String =
    s"CustomerEncryption(encryptionAlgorithm=$encryptionAlgorithm, keySha256=$keySha256)"
}

object CustomerEncryption {
  def apply(encryptionAlgorithm: String, keySha256: String): CustomerEncryption =
    new CustomerEncryption(encryptionAlgorithm, keySha256)

  def create(encryptionAlgorithm: String, keySha256: String): CustomerEncryption =
    new CustomerEncryption(encryptionAlgorithm, keySha256)
}
