/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
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
private[ftp] trait FtpOperations extends CommonFtpOperations { _: FtpLike[FTPClient, FtpSettings] =>

  def connect(connectionSettings: FtpSettings)(implicit ftpClient: FTPClient): Try[Handler] = Try {
    connectionSettings.proxy.foreach(ftpClient.setProxy)

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
