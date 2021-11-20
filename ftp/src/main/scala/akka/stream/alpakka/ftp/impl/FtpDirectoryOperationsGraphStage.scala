/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.ftp.impl

import akka.annotation.InternalApi
import akka.stream.alpakka.ftp.RemoteFileSettings
import akka.stream.stage.{GraphStageLogic, OutHandler}
import akka.stream.Attributes

@InternalApi
private[ftp] trait FtpDirectoryOperationsGraphStage[FtpClient, S <: RemoteFileSettings]
    extends FtpGraphStage[FtpClient, S, Unit] {
  val ftpLike: FtpLike[FtpClient, S]
  val directoryName: String

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new FtpGraphStageLogic(shape, ftpLike, connectionSettings, ftpClient) {
      setHandler(
        out,
        new OutHandler {
          override def onPull(): Unit = {
            push(out, ftpLike.mkdir(basePath, directoryName, handler.get))
            complete(out)
          }
        }
      )

      override protected def doPreStart(): Unit = ()

      override protected def matSuccess(): Boolean = true

      override protected def matFailure(t: Throwable): Boolean = true
    }
}
