/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.ftp
package impl

import akka.stream.impl.Stages.DefaultAttributes.IODispatcher
import akka.stream.stage.{GraphStageWithMaterializedValue, InHandler, OutHandler}
import akka.stream.{Attributes, IOResult, Inlet, Outlet, Shape, SinkShape, SourceShape}
import akka.util.ByteString
import akka.util.ByteString.ByteString1C

import scala.concurrent.{Future, Promise}
import java.io.{IOException, InputStream, OutputStream}

import akka.annotation.InternalApi

import scala.util.control.NonFatal

/**
 * INTERNAL API
 */
@InternalApi
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

/**
 * INTERNAL API
 */
@InternalApi
private[ftp] trait FtpIOSourceStage[FtpClient, S <: RemoteFileSettings]
    extends FtpIOGraphStage[FtpClient, S, SourceShape[ByteString]] {

  def chunkSize: Int

  def offset: Long = 0L

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
            try {
              readChunk() match {
                case Some(bs) =>
                  push(out, bs)
                case None =>
                  complete(out)
              }
            } catch {
              case NonFatal(e) =>
                failed = true
                matFailure(e)
                failStage(e)
            }
        }
      ) // end of handler

      override def postStop(): Unit =
        try {
          isOpt.foreach { os =>
            try {
              os.close()
              ftpLike match {
                case cfo: CommonFtpOperations =>
                  if (!cfo.completePendingCommand(handler.get.asInstanceOf[cfo.Handler]))
                    throw new IOException("File transfer failed.")
                case _ =>
              }
            } catch {
              case e: IOException =>
                matFailure(e)
                // If we failed, we have to expect the stream might already be dead
                // so swallow the IOException
                if (!failed) throw e
              case NonFatal(e) =>
                matFailure(e)
                throw e
            }
          }
        } finally {
          super.postStop()
        }

      protected[this] def doPreStart(): Unit =
        isOpt = ftpLike match {
          case ro: RetrieveOffset =>
            Some(ro.retrieveFileInputStream(path, handler.get.asInstanceOf[ro.Handler], offset).get)
          case _ =>
            Some(ftpLike.retrieveFileInputStream(path, handler.get).get)
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

/**
 * INTERNAL API
 */
@InternalApi
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
          override def onPush(): Unit =
            try {
              write(grab(in))
              pull(in)
            } catch {
              case NonFatal(e) ⇒
                failed = true
                matFailure(e)
                failStage(e)
            }

          override def onUpstreamFailure(exception: Throwable): Unit = {
            matFailure(exception)
            failed = true
            super.onUpstreamFailure(exception)
          }
        }
      ) // end of handler

      override def postStop(): Unit =
        try {
          osOpt.foreach { os =>
            try {
              os.close()
              ftpLike match {
                case cfo: CommonFtpOperations =>
                  if (!cfo.completePendingCommand(handler.get.asInstanceOf[cfo.Handler]))
                    throw new IOException("File transfer failed.")
                case _ =>
              }
            } catch {
              case e: IOException =>
                matFailure(e)
                // If we failed, we have to expect the stream might already be dead
                // so swallow the IOException
                if (!failed) throw e
              case NonFatal(e) =>
                matFailure(e)
                throw e
            }
          }
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

/**
 * INTERNAL API
 */
@InternalApi
private[ftp] trait FtpMoveSink[FtpClient, S <: RemoteFileSettings]
    extends GraphStageWithMaterializedValue[SinkShape[FtpFile], Future[IOResult]] {
  val connectionSettings: S
  val ftpClient: () => FtpClient
  val ftpLike: FtpLike[FtpClient, S]
  val destinationPath: FtpFile => String
  val in: Inlet[FtpFile] = Inlet("FtpMvSink")

  def shape: SinkShape[FtpFile] = SinkShape(in)

  def createLogicAndMaterializedValue(inheritedAttributes: Attributes) = {
    val matValuePromise = Promise[IOResult]()

    val logic = new FtpGraphStageLogic[FtpFile, FtpClient, S](shape, ftpLike, connectionSettings, ftpClient) {
      {
        setHandler(
          in,
          new InHandler {
            override def onPush(): Unit = {
              val sourcePath = grab(in)
              ftpLike.move(sourcePath.path, destinationPath(sourcePath), handler.get)
              pull(in)
            }
          }
        )
      }

      protected[this] def doPreStart(): Unit = pull(in)

      protected[this] def matSuccess(): Boolean =
        matValuePromise.trySuccess(IOResult.createSuccessful(1))

      protected[this] def matFailure(t: Throwable): Boolean =
        matValuePromise.trySuccess(IOResult.createFailed(1, t))
    } // end of stage logic

    (logic, matValuePromise.future)
  }
}

/**
 * INTERNAL API
 */
@InternalApi
private[ftp] trait FtpRemoveSink[FtpClient, S <: RemoteFileSettings]
    extends GraphStageWithMaterializedValue[SinkShape[FtpFile], Future[IOResult]] {
  val connectionSettings: S
  val ftpClient: () => FtpClient
  val ftpLike: FtpLike[FtpClient, S]
  val in: Inlet[FtpFile] = Inlet("FtpRmSink")

  def shape: SinkShape[FtpFile] = SinkShape(in)

  def createLogicAndMaterializedValue(inheritedAttributes: Attributes) = {
    val matValuePromise = Promise[IOResult]()
    val logic = new FtpGraphStageLogic[Unit, FtpClient, S](shape, ftpLike, connectionSettings, ftpClient) {
      {
        setHandler(in, new InHandler {
          override def onPush(): Unit = {
            ftpLike.remove(grab(in).path, handler.get)
            pull(in)
          }
        })
      }

      protected[this] def doPreStart(): Unit = pull(in)

      protected[this] def matSuccess(): Boolean =
        matValuePromise.trySuccess(IOResult.createSuccessful(1))

      protected[this] def matFailure(t: Throwable): Boolean =
        matValuePromise.trySuccess(IOResult.createFailed(1, t))
    } // end of stage logic

    (logic, matValuePromise.future)
  }
}
