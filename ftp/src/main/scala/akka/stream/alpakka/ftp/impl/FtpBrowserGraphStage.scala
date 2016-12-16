/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.ftp
package impl

import akka.stream.stage.{ GraphStage, OutHandler }
import akka.stream.{ Attributes, Outlet, SourceShape }
import akka.stream.impl.Stages.DefaultAttributes.IODispatcher

private[ftp] trait FtpBrowserGraphStage[FtpClient, S <: RemoteFileSettings] extends GraphStage[SourceShape[FtpFile]] {

  def name: String

  def basePath: String

  def connectionSettings: S

  def ftpClient: () => FtpClient

  val ftpLike: FtpLike[FtpClient, S]

  val shape: SourceShape[FtpFile] = SourceShape(Outlet[FtpFile](s"$name.out"))

  override def initialAttributes: Attributes =
    super.initialAttributes and Attributes.name(name) and IODispatcher

  def createLogic(inheritedAttributes: Attributes) = {
    val logic = new FtpGraphStageLogic[FtpFile, FtpClient, S](shape, ftpLike, connectionSettings, ftpClient) {

      private[this] var buffer: Seq[FtpFile] = Seq.empty[FtpFile]

      setHandler(out,
        new OutHandler {
        def onPull(): Unit = {
          fillBuffer()
          buffer match {
            case Seq() =>
              finalize()
            case head +: Seq() =>
              if (!head.isDirectory)
                push(out, head)
              finalize()
            case head +: tail =>
              buffer = tail
              if (!head.isDirectory)
                push(out, head)
              else
                onPull()
          }
          def finalize() =
            try {
              disconnect()
            } finally {
              complete(out)
            }
        } // end of onPull

        override def onDownstreamFinish(): Unit =
          try {
            disconnect()
          } finally {
            matSuccess()
            super.onDownstreamFinish()
          }
      }) // end of handler

      protected[this] def doPreStart(): Unit =
        buffer = initBuffer(basePath)

      override protected[this] def matSuccess() = true

      override protected[this] def matFailure(t: Throwable) = true

      private[this] def initBuffer(basePath: String) =
        getFilesFromPath(basePath)

      private[this] def fillBuffer() =
        buffer match {
          case Seq() => // Nothing to do
          case head +: tail =>
            if (head.isDirectory) {
              buffer = getFilesFromPath(head.path) ++ tail
            }
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
