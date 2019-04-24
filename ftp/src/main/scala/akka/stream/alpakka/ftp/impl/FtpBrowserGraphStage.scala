/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.ftp
package impl

import akka.annotation.InternalApi
import akka.stream.stage.{GraphStage, OutHandler}
import akka.stream.{Attributes, Outlet, SourceShape}
import akka.stream.impl.Stages.DefaultAttributes.IODispatcher

/**
 * INTERNAL API
 */
@InternalApi
private[ftp] trait FtpBrowserGraphStage[FtpClient, S <: RemoteFileSettings] extends GraphStage[SourceShape[FtpFile]] {

  def name: String

  def basePath: String

  def connectionSettings: S

  def ftpClient: () => FtpClient

  val ftpLike: FtpLike[FtpClient, S]

  val shape: SourceShape[FtpFile] = SourceShape(Outlet[FtpFile](s"$name.out"))

  val out = shape.outlets.head.asInstanceOf[Outlet[FtpFile]]

  val branchSelector: FtpFile => Boolean = (f) => true

  val emitTraversedDirectories: Boolean

  override def initialAttributes: Attributes =
    super.initialAttributes and Attributes.name(name) and IODispatcher

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

          override def onDownstreamFinish(): Unit = {
            matSuccess()
            super.onDownstreamFinish()
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
