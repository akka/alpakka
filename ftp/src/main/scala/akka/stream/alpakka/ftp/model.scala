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

  /**
   * SFTP settings
   *
   * @param host host
   * @param port port
   * @param credentials credentials (username and password)
   * @param strictHostKeyChecking sets whether to use strict host key checking.
   */
  final case class SftpSettings(
      host: InetAddress,
      port: Int = DefaultSftpPort,
      credentials: FtpCredentials = AnonFtpCredentials,
      strictHostKeyChecking: Boolean = true
  ) extends RemoteFileSettings
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
   * @param username the username
   * @param password the password
   */
  final case class NonAnonFtpCredentials(username: String, password: String) extends FtpCredentials
}
