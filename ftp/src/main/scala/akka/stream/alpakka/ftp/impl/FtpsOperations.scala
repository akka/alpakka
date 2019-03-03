/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.ftp.impl

import akka.annotation.InternalApi
import akka.stream.alpakka.ftp.FtpsSettings
import org.apache.commons.net.ftp.{FTP, FTPSClient}

import scala.util.Try

/**
 * INTERNAL API
 */
@InternalApi
private[ftp] trait FtpsOperations extends CommonFtpOperations { _: FtpLike[FTPSClient, FtpsSettings] =>

  def connect(connectionSettings: FtpsSettings)(implicit ftpClient: FTPSClient): Try[Handler] = Try {
    ftpClient.connect(connectionSettings.host, connectionSettings.port)

    connectionSettings.configureConnection(ftpClient)

    ftpClient.login(
      connectionSettings.credentials.username,
      connectionSettings.credentials.password
    )

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
