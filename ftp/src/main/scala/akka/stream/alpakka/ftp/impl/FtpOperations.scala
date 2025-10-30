/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.ftp
package impl

import akka.annotation.InternalApi
import org.apache.commons.net.ftp.{FTP, FTPClient}

import scala.util.Try

/**
 * INTERNAL API
 */
@InternalApi
private[ftp] trait FtpOperations extends CommonFtpOperations { ftpLike: FtpLike[FTPClient, FtpSettings] =>

  def connect(connectionSettings: FtpSettings)(implicit ftpClient: FTPClient): Try[Handler] = Try {
    connectionSettings.proxy.foreach(ftpClient.setProxy)

    if (ftpClient.getAutodetectUTF8() != connectionSettings.autodetectUTF8) {
      ftpClient.setAutodetectUTF8(connectionSettings.autodetectUTF8)
    }

    try {
      ftpClient.connect(connectionSettings.host, connectionSettings.port)
    } catch {
      case e: java.net.ConnectException =>
        throw new java.net.ConnectException(
          e.getMessage + s" host=[${connectionSettings.host}], port=${connectionSettings.port} ${connectionSettings.proxy
            .map("proxy=" + _.toString)
            .getOrElse("")}"
        )
    }

    connectionSettings.configureConnection(ftpClient)

    ftpClient.login(
      connectionSettings.credentials.username,
      connectionSettings.credentials.password
    )
    if (ftpClient.getReplyCode == 530) {
      throw new FtpAuthenticationException(
        s"unable to login to host=[${connectionSettings.host}], port=${connectionSettings.port} ${connectionSettings.proxy
          .fold("")("proxy=" + _.toString)}"
      )
    }

    if (connectionSettings.binary) {
      ftpClient.setFileType(FTP.BINARY_FILE_TYPE)
    }

    if (connectionSettings.passiveMode) {
      ftpClient.enterLocalPassiveMode()
    }

    ftpClient
  }

  def disconnect(handler: Handler)(implicit ftpClient: FTPClient): Unit =
    if (ftpClient.isConnected) {
      ftpClient.logout()
      ftpClient.disconnect()
    }
}
