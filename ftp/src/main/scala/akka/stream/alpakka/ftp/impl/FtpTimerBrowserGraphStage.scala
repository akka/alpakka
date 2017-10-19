/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.ftp.impl

import akka.stream.Attributes
import akka.stream.alpakka.ftp.{FtpFile, RemoteFileSettings}
import akka.stream.stage.{OutHandler, TimerGraphStageLogic}

import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

trait FtpTimerBrowserGraphStage[FtpClient, S <: RemoteFileSettings] extends FtpBrowserGraphStage[FtpClient, S] {

  def pollInterval: FiniteDuration

  override def createLogic(inheritedAttributes: Attributes) = {

    val logic = new TimerGraphStageLogic(shape) {

      private[this] var buffer: Seq[FtpFile] = Seq.empty[FtpFile]
      private[this] implicit val client: FtpClient = ftpClient()
      private[this] var handler: Option[ftpLike.Handler] = Option.empty[ftpLike.Handler]

      setHandler(
        out,
        new OutHandler {
          def onPull(): Unit = {
            fillBuffer(buffer)
            buffer match {
              case head +: tail =>
                buffer = tail
                push(out, head)
              case _ =>
                schedulePoll()
            }
          }
          override def onDownstreamFinish(): Unit = {
            matSuccess()
            disconnect()
            super.onDownstreamFinish()
          }
        }
      )

      override def preStart(): Unit = {
        super.preStart()
        try {
          val tryConnect = ftpLike.connect(connectionSettings)
          if (tryConnect.isSuccess) {
            handler = tryConnect.toOption
          } else
            tryConnect.failed.foreach { case NonFatal(t) => throw t }
          doPreStart()
        } catch {
          case NonFatal(t) =>
            matFailure(t)
            failStage(t)
        }
      }

      protected[this] def doPreStart(): Unit = startBuffer(basePath)

      protected[this] def disconnect(): Unit = handler.foreach(ftpLike.disconnect)

      protected[this] def matSuccess(): Boolean = true

      protected[this] def matFailure(t: Throwable): Boolean = true

      private[this] def startBuffer(bp: String = basePath): Unit = {
        val res = getFilesFromPath(bp)
        fillBuffer(res)
        buffer = res
      }

      @scala.annotation.tailrec
      private[this] def fillBuffer(in: Seq[FtpFile]): Unit = in match {
        case head +: tail if head.isDirectory && branchSelector(head) =>
          val res = getFilesFromPath(head.path)
          buffer = res
          fillBuffer(res ++ tail)
        case _ =>
      }

      private[this] def getFilesFromPath(bp: String) =
        if (bp.isEmpty) ftpLike.listFiles(handler.get)
        else ftpLike.listFiles(bp, handler.get)

      override def onTimer(timerKey: Any): Unit =
        if (!isClosed(out)) {
          buffer = Seq.empty[FtpFile]
          startBuffer(basePath)
          if (buffer.nonEmpty) pushHead()
          else schedulePoll()
        }

      private def pushHead(): Unit =
        buffer.headOption.foreach(push(out, _))

      private def schedulePoll(): Unit =
        scheduleOnce("poll", pollInterval)
    }
    logic
  }
}
