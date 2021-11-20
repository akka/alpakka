/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.ftp
package impl

import akka.annotation.InternalApi
import akka.stream.Attributes
import akka.stream.stage.OutHandler

/**
 * INTERNAL API
 */
@InternalApi
private[ftp] trait FtpBrowserGraphStage[FtpClient, S <: RemoteFileSettings]
    extends FtpGraphStage[FtpClient, S, FtpFile] {
  val ftpLike: FtpLike[FtpClient, S]

  val branchSelector: FtpFile => Boolean = (f) => true

  def emitTraversedDirectories: Boolean = false

  def createLogic(inheritedAttributes: Attributes) = {
    val logic = new FtpGraphStageLogic[FtpFile, FtpClient, S](shape, ftpLike, connectionSettings, ftpClient) {

      private[this] var buffer: Seq[FtpFile] = Seq.empty[FtpFile]

      private[this] var traversed: Seq[FtpFile] = Seq.empty[FtpFile]

      setHandler(
        out,
        new OutHandler {
          def onPull(): Unit = {
            fillBuffer()
            traversed match {
              case head +: tail =>
                traversed = tail
                push(out, head)
              case _ =>
                buffer match {
                  case head +: tail =>
                    buffer = tail
                    push(out, head)
                  case _ => complete(out)
                }
            }
          } // end of onPull

          override def onDownstreamFinish(cause: Throwable): Unit = {
            matSuccess()
            super.onDownstreamFinish(cause)
          }
        }
      ) // end of handler

      protected[this] def doPreStart(): Unit =
        buffer = initBuffer(basePath)

      override protected[this] def matSuccess() = true

      override protected[this] def matFailure(t: Throwable) = true

      private[this] def initBuffer(basePath: String) =
        getFilesFromPath(basePath)

      @scala.annotation.tailrec
      private[this] def fillBuffer(): Unit = buffer match {
        case head +: tail if head.isDirectory && branchSelector(head) =>
          buffer = getFilesFromPath(head.path) ++ tail
          if (emitTraversedDirectories) traversed = traversed :+ head
          fillBuffer()
        case _ => // do nothing
      }

      private[this] def getFilesFromPath(basePath: String) =
        if (basePath.isEmpty)
          ftpLike.listFiles(handler.get)
        else
          ftpLike.listFiles(basePath, handler.get)

    } // end of stage logic

    logic
  }
}
