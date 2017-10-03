/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.oracle.bmcs.auth

import java.io.{FileInputStream, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.security.interfaces.RSAPrivateKey

import org.bouncycastle.asn1.pkcs.PrivateKeyInfo
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo
import org.bouncycastle.openssl.jcajce.{JcaPEMKeyConverter, JcePEMDecryptorProviderBuilder}
import org.bouncycastle.openssl.{PEMEncryptedKeyPair, PEMKeyPair, PEMParser}

sealed trait BmcsCredentials {
  def rsaPrivateKey: RSAPrivateKey

  def keyId: String
}

final case class BasicCredentials(userOcid: String,
                                  tenancyOcid: String,
                                  rsaPrivateKey: RSAPrivateKey,
                                  keyFingerprint: String)
    extends BmcsCredentials {
  val keyId: String = s"$tenancyOcid/$userOcid/$keyFingerprint"
}

object BasicCredentials {

  private val converter = new JcaPEMKeyConverter

  def apply(userOcid: String,
            tenancyOcid: String,
            keyPath: String,
            passphrase: Option[String],
            keyFingerprint: String): BmcsCredentials = {
    val rsaPrivateKey = fromPemFile(keyPath, passphrase)
    BasicCredentials(userOcid, tenancyOcid, rsaPrivateKey, keyFingerprint)
  }

  def apply(userOcid: String,
            tenancyOcid: String,
            region: String,
            privateKey: RSAPrivateKey,
            keyFingerprint: String): BmcsCredentials =
    BasicCredentials(userOcid, tenancyOcid, privateKey, keyFingerprint)

  //TODO: use scala ARM to close the stream when done.
  def fromPemFile(keyPath: String, passphrase: Option[String]): RSAPrivateKey =
    try {
      val keyStream = new FileInputStream(keyPath)
      val keyReader = new PEMParser(new InputStreamReader(keyStream, StandardCharsets.UTF_8))
      val obj = keyReader.readObject
      keyReader.close()
      keyStream.close()
      val keyInfo: PrivateKeyInfo = obj match {
        case pemEncryptedPair: PEMEncryptedKeyPair =>
          val password = passphrase match {
            case None => throw new RuntimeException(" This file needs a passphrase")
            case Some(x) => x
          }
          val decProv = new JcePEMDecryptorProviderBuilder().build(password.toCharArray)
          pemEncryptedPair.decryptKeyPair(decProv).getPrivateKeyInfo
        case privateKeyInfo: PrivateKeyInfo => privateKeyInfo
        case pemKeyPair: PEMKeyPair => pemKeyPair.getPrivateKeyInfo
        case publicKey: SubjectPublicKeyInfo =>
          throw new IllegalArgumentException("Public key provided instead of private key")
        case _ => throw new IllegalArgumentException("Private key must be in PEM format, was: " + obj.getClass)
      }
      converter.getPrivateKey(keyInfo).asInstanceOf[RSAPrivateKey]
    }

  def create(userOcid: String,
             tenancyOcid: String,
             keyPath: String,
             passphrase: String,
             keyFingerprint: String): BmcsCredentials =
    if (passphrase.isEmpty) {
      BasicCredentials(userOcid, tenancyOcid, keyPath, None, keyFingerprint)
    } else {
      BasicCredentials(userOcid, tenancyOcid, keyPath, Some(passphrase), keyFingerprint)
    }
}
