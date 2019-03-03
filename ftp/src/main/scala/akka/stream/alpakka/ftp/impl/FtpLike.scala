/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.ftp
package impl

import net.schmizz.sshj.SSHClient
import org.apache.commons.net.ftp.{FTPClient, FTPSClient}

import scala.collection.immutable
import scala.util.Try
import java.io.{InputStream, OutputStream}

import akka.annotation.InternalApi

/**
 * INTERNAL API
 */
@InternalApi
protected[ftp] trait FtpLike[FtpClient, S <: RemoteFileSettings] {

  type Handler

  def connect(connectionSettings: S)(implicit ftpClient: FtpClient): Try[Handler]

  def disconnect(handler: Handler)(implicit ftpClient: FtpClient): Unit

  def listFiles(basePath: String, handler: Handler): immutable.Seq[FtpFile]

  def listFiles(handler: Handler): immutable.Seq[FtpFile]

  def retrieveFileInputStream(name: String, handler: Handler): Try[InputStream]

  def storeFileOutputStream(name: String, handler: Handler, append: Boolean): Try[OutputStream]

  def move(fromPath: String, destinationPath: String, handler: Handler): Unit

  def remove(path: String, handler: Handler): Unit
}

/**
 * INTERNAL API
 */
@InternalApi
object FtpLike {
  // type class instances
  implicit val ftpLikeInstance = new FtpLike[FTPClient, FtpSettings] with FtpOperations
  implicit val ftpsLikeInstance = new FtpLike[FTPSClient, FtpsSettings] with FtpsOperations
  implicit val sFtpLikeInstance = new FtpLike[SSHClient, SftpSettings] with SftpOperations
}
