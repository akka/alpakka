/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.ftp
package impl

import com.jcraft.jsch.JSch
import org.apache.commons.net.ftp.FTPClient
import scala.collection.immutable
import scala.util.Try
import java.io.InputStream

protected[ftp] trait FtpLike[FtpClient] {

  type Handler

  def connect(connectionSettings: RemoteFileSettings)(implicit ftpClient: FtpClient): Try[Handler]

  def disconnect(handler: Handler)(implicit ftpClient: FtpClient): Unit

  def listFiles(basePath: String, handler: Handler): immutable.Seq[FtpFile]

  def listFiles(handler: Handler): immutable.Seq[FtpFile]

  def retrieveFileInputStream(name: String, handler: Handler): Try[InputStream]
}

object FtpLike {
  // type class instances
  implicit val ftpLikeInstance = new FtpLike[FTPClient] with FtpOperations
  implicit val sFtpLikeInstance = new FtpLike[JSch] with SftpOperations
}
