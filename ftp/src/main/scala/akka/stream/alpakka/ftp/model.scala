/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.ftp

import akka.stream.alpakka.ftp.FtpCredentials.AnonFtpCredentials
import java.net.InetAddress
import java.nio.file.attribute.PosixFilePermission

/**
 * FTP remote file descriptor.
 *
 * @param name file name
 * @param path remote file path as viewed by the logged user.
 *             It should always start by '/'
 * @param isDirectory the descriptor is a directory
 * @param size the file size in bytes
 * @param lastModified the timestamp of the file last modification
 * @param permissions the permissions of the file
 */
final case class FtpFile(
    name: String,
    path: String,
    isDirectory: Boolean,
    size: Long,
    lastModified: Long,
    permissions: Set[PosixFilePermission]
) {
  val isFile: Boolean = !this.isDirectory
}

/**
 * Common remote file settings.
 */
sealed abstract class RemoteFileSettings extends Product with Serializable {
  def host: InetAddress
  def port: Int
  def credentials: FtpCredentials
}

/**
 * Common settings for FTP and FTPs.
 */
sealed abstract class FtpFileSettings extends RemoteFileSettings {
  def binary: Boolean // BINARY or ASCII (default)
  def passiveMode: Boolean
}

/**
 * FTP settings
 *
 * @param host host
 * @param port port
 * @param credentials credentials (username and password)
 * @param binary specifies the file transfer mode, BINARY or ASCII. Default is ASCII (false)
 * @param passiveMode specifies whether to use passive mode connections. Default is active mode (false)
 */
final case class FtpSettings(
    host: InetAddress,
    port: Int = FtpSettings.DefaultFtpPort,
    credentials: FtpCredentials = AnonFtpCredentials,
    binary: Boolean = false,
    passiveMode: Boolean = false
) extends FtpFileSettings {
  def withPort(port: Int): FtpSettings =
    copy(port = port)

  def withCredentials(credentials: FtpCredentials): FtpSettings =
    copy(credentials = credentials)

  def withBinary(binary: Boolean): FtpSettings =
    copy(binary = binary)

  def withPassiveMode(passiveMode: Boolean): FtpSettings =
    copy(passiveMode = passiveMode)
}

object FtpSettings {

  /** Default FTP port */
  final val DefaultFtpPort = 21

  /** Java API */
  def create(host: InetAddress): FtpSettings =
    FtpSettings(host)
}

/**
 * FTPs settings
 *
 * @param host host
 * @param port port
 * @param credentials credentials (username and password)
 * @param binary specifies the file transfer mode, BINARY or ASCII. Default is ASCII (false)
 * @param passiveMode specifies whether to use passive mode connections. Default is active mode (false)
 */
final case class FtpsSettings(
    host: InetAddress,
    port: Int = FtpsSettings.DefaultFtpsPort,
    credentials: FtpCredentials = AnonFtpCredentials,
    binary: Boolean = false,
    passiveMode: Boolean = false
) extends FtpFileSettings {
  def withPort(port: Int): FtpsSettings =
    copy(port = port)

  def withCredentials(credentials: FtpCredentials): FtpsSettings =
    copy(credentials = credentials)

  def withBinary(binary: Boolean): FtpsSettings =
    copy(binary = binary)

  def withPassiveMode(passiveMode: Boolean): FtpsSettings =
    copy(passiveMode = passiveMode)
}

object FtpsSettings {

  /** Default FTPs port */
  final val DefaultFtpsPort = 2222

  /** Java API */
  def create(host: InetAddress): FtpsSettings =
    FtpsSettings(host)
}

/**
 *
 * @param host host
 * @param port port
 * @param credentials credentials (username and password)
 * @param strictHostKeyChecking sets whether to use strict host key checking.
 * @param knownHosts known hosts file to be used when connecting
 * @param sftpIdentity private/public key config to use when connecting
 */
final case class SftpSettings(
    host: InetAddress,
    port: Int = SftpSettings.DefaultSftpPort,
    credentials: FtpCredentials = AnonFtpCredentials,
    strictHostKeyChecking: Boolean = true,
    knownHosts: Option[String] = None,
    sftpIdentity: Option[SftpIdentity] = None
) extends RemoteFileSettings {
  def withPort(port: Int): SftpSettings =
    copy(port = port)

  def withCredentials(credentials: FtpCredentials): SftpSettings =
    copy(credentials = credentials)

  def withStrictHostKeyChecking(strictHostKeyChecking: Boolean): SftpSettings =
    copy(strictHostKeyChecking = strictHostKeyChecking)

  def withKnownHosts(knownHosts: String): SftpSettings =
    copy(knownHosts = Some(knownHosts))

  def withSftpIdentity(sftpIdentity: SftpIdentity): SftpSettings =
    copy(sftpIdentity = Some(sftpIdentity))
}

object SftpSettings {

  /** Default SFTP port */
  final val DefaultSftpPort = 22

  /** Java API */
  def create(host: InetAddress): SftpSettings =
    SftpSettings(host)
}

/**
 * FTP credentials
 */
sealed abstract class FtpCredentials extends Product with Serializable {
  def username: String
  def password: String
}
object FtpCredentials {
  final val Anonymous = "anonymous"

  /** Java API */
  def createAnonCredentials() = AnonFtpCredentials

  /**
   * Anonymous credentials
   */
  case object AnonFtpCredentials extends FtpCredentials {
    val username = Anonymous
    val password = Anonymous
  }

  /**
   * Non-anonymous credentials
   *
   * @param username the username
   * @param password the password
   */
  final case class NonAnonFtpCredentials(username: String, password: String) extends FtpCredentials
}

object SftpIdentity {

  /** Java API */
  def createRawSftpIdentity(privateKey: Array[Byte]): RawKeySftpIdentity =
    RawKeySftpIdentity(privateKey)

  def createRawSftpIdentity(privateKey: Array[Byte], privateKeyFilePassphrase: Array[Byte]): RawKeySftpIdentity =
    RawKeySftpIdentity(privateKey, Some(privateKeyFilePassphrase))

  def createRawSftpIdentity(
      privateKey: Array[Byte],
      privateKeyFilePassphrase: Array[Byte],
      publicKey: Array[Byte]
  ): RawKeySftpIdentity =
    RawKeySftpIdentity(privateKey, Some(privateKeyFilePassphrase), Some(publicKey))

  def createFileSftpIdentity(privateKey: String): KeyFileSftpIdentity =
    KeyFileSftpIdentity(privateKey)

  def createFileSftpIdentity(privateKey: String, privateKeyFilePassphrase: Array[Byte]): KeyFileSftpIdentity =
    KeyFileSftpIdentity(privateKey, Some(privateKeyFilePassphrase))
}

sealed abstract class SftpIdentity {
  type KeyType
  val privateKey: KeyType
  val privateKeyFilePassphrase: Option[Array[Byte]]
}

/**
 * SFTP identity for authenticating using private/public key value
 *
 * @param privateKey private key value to use when connecting
 * @param privateKeyFilePassphrase password to use to decrypt private key
 * @param publicKey public key value to use when connecting
 */
final case class RawKeySftpIdentity(
    privateKey: Array[Byte],
    privateKeyFilePassphrase: Option[Array[Byte]] = None,
    publicKey: Option[Array[Byte]] = None
) extends SftpIdentity {
  type KeyType = Array[Byte]

  def withPrivateKeyFilePassphrase(privateKeyFilePassphrase: Array[Byte]): RawKeySftpIdentity =
    copy(privateKeyFilePassphrase = Some(privateKeyFilePassphrase))

  def withPublicKey(publicKey: KeyType): RawKeySftpIdentity =
    copy(publicKey = Some(publicKey))
}

/**
 * SFTP identity for authenticating using private/public key file
 *
 * @param privateKey private key file to use when connecting
 * @param privateKeyFilePassphrase password to use to decrypt private key file
 */
final case class KeyFileSftpIdentity(
    privateKey: String,
    privateKeyFilePassphrase: Option[Array[Byte]] = None
) extends SftpIdentity {
  type KeyType = String

  def withPrivateKeyFilePassphrase(privateKeyFilePassphrase: Array[Byte]): KeyFileSftpIdentity =
    copy(privateKeyFilePassphrase = Some(privateKeyFilePassphrase))
}
