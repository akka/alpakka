/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.ftp
package impl

import akka.stream.stage.{ GraphStageWithMaterializedValue, OutHandler }
import akka.stream.{ Attributes, IOResult, Outlet, SourceShape }
import akka.stream.impl.Stages.DefaultAttributes.IODispatcher
import akka.util.ByteString
import akka.util.ByteString.ByteString1C
import scala.concurrent.{ Future, Promise }
import scala.util.control.NonFatal
import java.io.InputStream
import java.nio.file.Path

private[ftp] trait FtpIOGraphStage[FtpClient, S <: RemoteFileSettings]
    extends GraphStageWithMaterializedValue[SourceShape[ByteString], Future[IOResult]] {

  def name: String

  def path: Path

  def chunkSize: Int

  def connectionSettings: S

  implicit def ftpClient: () => FtpClient

  val ftpLike: FtpLike[FtpClient, S]

  override def initialAttributes: Attributes =
    super.initialAttributes and Attributes.name(name) and IODispatcher

  val shape = SourceShape(Outlet[ByteString](s"$name.out"))

  def createLogicAndMaterializedValue(inheritedAttributes: Attributes) = {

    val matValuePromise = Promise[IOResult]()

    val logic = new FtpGraphStageLogic[ByteString, FtpClient, S](shape, ftpLike, connectionSettings, ftpClient) {

      private[this] var isOpt: Option[InputStream] = None
      private[this] var readBytesTotal: Long = 0L

      setHandler(out,
        new OutHandler {
        def onPull(): Unit =
          readChunk() match {
            case Some(bs) =>
              push(out, bs)
            case None =>
              try {
                isOpt.foreach(_.close())
                disconnect()
              } finally {
                matSuccess()
                complete(out)
              }
          }

        override def onDownstreamFinish(): Unit =
          try {
            isOpt.foreach(_.close())
            disconnect()
          } finally {
            matSuccess()
            super.onDownstreamFinish()
          }
      }) // end of handler

      override def postStop(): Unit =
        try {
          isOpt.foreach(_.close())
        } finally {
          super.postStop()
        }

      protected[this] def doPreStart(): Unit = {
        val tryIs = ftpLike.retrieveFileInputStream(path.toAbsolutePath.toString, handler.get)
        if (tryIs.isSuccess)
          isOpt = tryIs.toOption
        else
          tryIs.failed.foreach { case NonFatal(t) => throw t }
      }

      protected[this] def matSuccess(): Boolean =
        matValuePromise.trySuccess(IOResult.createSuccessful(readBytesTotal))

      protected[this] def matFailure(t: Throwable): Boolean =
        matValuePromise.trySuccess(IOResult.createFailed(readBytesTotal, t))

      /** BLOCKING I/O READ */
      private[this] def readChunk() = {
        def read(arr: Array[Byte]) =
          isOpt.flatMap { is =>
            val readBytes = is.read(arr)
            if (readBytes > -1) Some(readBytes)
            else None
          }
        val arr = Array.ofDim[Byte](chunkSize)
        read(arr).map { readBytes =>
          readBytesTotal += readBytes
          if (readBytes == chunkSize)
            ByteString1C(arr)
          else
            ByteString1C(arr).take(readBytes)
        }
      }

    } // end of stage logic

    (logic, matValuePromise.future)
  }

}
