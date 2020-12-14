/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.ftp.impl

import akka.annotation.InternalApi
import akka.stream.alpakka.ftp.{FtpAuthenticationException, FtpsSettings}
import org.apache.commons.net.ftp.{FTP, FTPSClient}

import scala.util.Try

/**
 * INTERNAL API
 */
@InternalApi
private[ftp] trait FtpsOperations extends CommonFtpOperations {
  _: FtpLike[FTPSClient, FtpsSettings] =>

  def connect(connectionSettings: FtpsSettings)(implicit ftpClient: FTPSClient): Try[Handler] =
    Try {
      connectionSettings.proxy.foreach(ftpClient.setProxy)

      ftpClient.connect(connectionSettings.host, connectionSettings.port)

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

  def disconnect(handler: Handler)(implicit ftpClient: FTPSClient): Unit =
    if (ftpClient.isConnected) {
      ftpClient.logout()
      ftpClient.disconnect()
    }
}
