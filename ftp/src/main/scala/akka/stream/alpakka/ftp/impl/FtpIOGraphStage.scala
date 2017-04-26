/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.ftp
package impl

import akka.stream.impl.Stages.DefaultAttributes.IODispatcher
import akka.stream.stage.{GraphStageWithMaterializedValue, InHandler, OutHandler}
import akka.stream.{Attributes, IOResult, Inlet, Outlet, Shape, SinkShape, SourceShape}
import akka.util.ByteString
import akka.util.ByteString.ByteString1C
import scala.concurrent.{Future, Promise}
import java.io.{InputStream, OutputStream}

private[ftp] trait FtpIOGraphStage[FtpClient, S <: RemoteFileSettings, Sh <: Shape]
    extends GraphStageWithMaterializedValue[Sh, Future[IOResult]] {

  def name: String

  def path: String

  def connectionSettings: S

  implicit def ftpClient: () => FtpClient

  val ftpLike: FtpLike[FtpClient, S]

  override def initialAttributes: Attributes =
    super.initialAttributes and Attributes.name(name) and IODispatcher

  override def shape: Sh
}

private[ftp] trait FtpIOSourceStage[FtpClient, S <: RemoteFileSettings]
    extends FtpIOGraphStage[FtpClient, S, SourceShape[ByteString]] {

  def chunkSize: Int

  val shape: SourceShape[ByteString] = SourceShape(Outlet[ByteString](s"$name.out"))
  val out: Outlet[ByteString] = shape.outlets.head.asInstanceOf[Outlet[ByteString]]

  def createLogicAndMaterializedValue(inheritedAttributes: Attributes) = {

    val matValuePromise = Promise[IOResult]()

    val logic = new FtpGraphStageLogic[ByteString, FtpClient, S](shape, ftpLike, connectionSettings, ftpClient) {

      private[this] var isOpt: Option[InputStream] = None
      private[this] var readBytesTotal: Long = 0L

      setHandler(
        out,
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
        }
      ) // end of handler

      override def postStop(): Unit =
        try {
          isOpt.foreach(_.close())
        } finally {
          super.postStop()
        }

      protected[this] def doPreStart(): Unit =
        isOpt = Some(ftpLike.retrieveFileInputStream(path, handler.get).get)

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

private[ftp] trait FtpIOSinkStage[FtpClient, S <: RemoteFileSettings]
    extends FtpIOGraphStage[FtpClient, S, SinkShape[ByteString]] {

  def append: Boolean

  val shape: SinkShape[ByteString] = SinkShape(Inlet[ByteString](s"$name.in"))
  val in: Inlet[ByteString] = shape.inlets.head.asInstanceOf[Inlet[ByteString]]

  def createLogicAndMaterializedValue(inheritedAttributes: Attributes) = {

    val matValuePromise = Promise[IOResult]()

    val logic = new FtpGraphStageLogic[ByteString, FtpClient, S](shape, ftpLike, connectionSettings, ftpClient) {

      private[this] var osOpt: Option[OutputStream] = None
      private[this] var writtenBytesTotal: Long = 0L

      setHandler(
        in,
        new InHandler {
          override def onPush(): Unit = {
            write(grab(in))
            pull(in)
          }

          override def onUpstreamFinish(): Unit =
            try {
              osOpt.foreach(_.close())
              disconnect()
            } finally {
              matSuccess()
              super.onUpstreamFinish()
            }

          override def onUpstreamFailure(exception: Throwable): Unit = {
            matFailure(exception)
            failStage(exception)
          }
        }
      ) // end of handler

      override def postStop(): Unit =
        try {
          osOpt.foreach(_.close())
        } finally {
          super.postStop()
        }

      protected[this] def doPreStart(): Unit = {
        osOpt = Some(ftpLike.storeFileOutputStream(path, handler.get, append).get)
        pull(in)
      }

      protected[this] def matSuccess(): Boolean =
        matValuePromise.trySuccess(IOResult.createSuccessful(writtenBytesTotal))

      protected[this] def matFailure(t: Throwable): Boolean =
        matValuePromise.trySuccess(IOResult.createFailed(writtenBytesTotal, t))

      /** BLOCKING I/O WRITE */
      private[this] def write(bytes: ByteString) =
        osOpt.foreach { os =>
          os.write(bytes.toArray)
          writtenBytesTotal += bytes.size
        }

    } // end of stage logic

    (logic, matValuePromise.future)
  }

}
