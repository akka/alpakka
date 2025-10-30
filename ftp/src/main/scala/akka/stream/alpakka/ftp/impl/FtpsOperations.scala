/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
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
  ftpLike: FtpLike[FTPSClient, FtpsSettings] =>

  def connect(connectionSettings: FtpsSettings)(implicit ftpClient: FTPSClient): Try[Handler] =
    Try {
      connectionSettings.proxy.foreach(ftpClient.setProxy)

      connectionSettings.keyManager.foreach(ftpClient.setKeyManager)
      connectionSettings.trustManager.foreach(ftpClient.setTrustManager)

      if (ftpClient.getAutodetectUTF8() != connectionSettings.autodetectUTF8) {
        ftpClient.setAutodetectUTF8(connectionSettings.autodetectUTF8)
      }

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
