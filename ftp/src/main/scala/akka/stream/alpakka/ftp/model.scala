/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.ftp

import akka.stream.alpakka.ftp.FtpCredentials.AnonFtpCredentials

import scala.language.implicitConversions
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

object RemoteFileSettings {

  /** Default FTP port */
  final val DefaultFtpPort = 21

  /** Default FTPs port */
  final val DefaultFtpsPort = 2222

  /** Default SFTP port */
  final val DefaultSftpPort = 22

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
      port: Int = DefaultFtpPort,
      credentials: FtpCredentials = AnonFtpCredentials,
      binary: Boolean = false,
      passiveMode: Boolean = false
  ) extends FtpFileSettings

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
      port: Int = DefaultFtpsPort,
      credentials: FtpCredentials = AnonFtpCredentials,
      binary: Boolean = false,
      passiveMode: Boolean = false
  ) extends FtpFileSettings

}

/**
 *
 * @param host host
 * @param port port
 * @param credentials credentials (username and password)
 * @param strictHostKeyChecking sets whether to use strict host key checking.
 * @param knownHosts known hosts file to be used when connecting
 * @param sftpIdentity private/public key config to use when connecting
 * @param options additional options for ssh connection
 */
final case class SftpSettings(
    host: InetAddress,
    port: Int = RemoteFileSettings.DefaultSftpPort,
    credentials: FtpCredentials = AnonFtpCredentials,
    strictHostKeyChecking: Boolean = true,
    knownHosts: Option[String] = None,
    sftpIdentity: Option[SftpIdentity] = None,
    options: Map[String, String] = Map.empty
) extends RemoteFileSettings {
  def withPort(port: Int) = copy(port = port)

  def withCredentials(credentials: FtpCredentials) = copy(credentials = credentials)

  def withStrictHostKeyChecking(strictHostKeyChecking: Boolean) = copy(strictHostKeyChecking = strictHostKeyChecking)

  def withKnownHosts(knownHosts: String) = copy(knownHosts = Some(knownHosts))

  def withSftpIdentity(sftpIdentity: SftpIdentity) = copy(sftpIdentity = Some(sftpIdentity))

  def withOptions(option: (String, String), options: (String, String)*) =
    copy(options = (option +: options).toMap)

  @annotation.varargs
  def withOptions(option: akka.japi.Pair[String, String], options: akka.japi.Pair[String, String]*) =
    copy(options = (option +: options).map(_.toScala).toMap)

}

object SftpSettings {
  def create(host: InetAddress) = SftpSettings(host)

  def createEmptyIdentity(): Option[SftpIdentity] = None

  def createEmptyKnownHosts(): Option[String] = None
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
  def createRawSftpIdentity(name: String, privateKey: Array[Byte]): RawKeySftpIdentity =
    RawKeySftpIdentity(name, privateKey)

  def createFileSftpIdentity(privateKey: String): KeyFileSftpIdentity =
    KeyFileSftpIdentity(privateKey)
}

sealed abstract class SftpIdentity {
  type KeyType
  val privateKey: KeyType
  val publicKey: Option[KeyType]
  val password: Option[Array[Byte]]
}

/**
 * SFTP identity for authenticating using private/public key value
 *
 * @param name name of identity
 * @param privateKey private key value to use when connecting
 * @param password password to use to decrypt private key
 * @param publicKey public key value to use when connecting
 */
final case class RawKeySftpIdentity(name: String,
                                    privateKey: Array[Byte],
                                    password: Option[Array[Byte]] = None,
                                    publicKey: Option[Array[Byte]] = None)
    extends SftpIdentity {

  override type KeyType = Array[Byte]

  def withPassword(password: Array[Byte]) = copy(password = Some(password))

  def withPublicKey(publicKey: KeyType) = copy(publicKey = Some(publicKey))
}

/**
 * SFTP identity for authenticating using private/public key file
 *
 * @param privateKey private key file to use when connecting
 * @param password password to use to decrypt private key file
 * @param publicKey public key file to use when connecting
 */
final case class KeyFileSftpIdentity(privateKey: String,
                                     password: Option[Array[Byte]] = None,
                                     publicKey: Option[String] = None)
    extends SftpIdentity {

  override type KeyType = String

  def withPassword(password: Array[Byte]) = copy(password = Some(password))

  def withPublicKey(publicKey: String) = copy(publicKey = Some(publicKey))
}
