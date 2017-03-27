/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.ftp
package impl

import akka.stream.Shape
import akka.stream.stage.GraphStageLogic

import scala.util.control.NonFatal

private[ftp] abstract class FtpGraphStageLogic[T, FtpClient, S <: RemoteFileSettings](
    val shape: Shape,
    val ftpLike: FtpLike[FtpClient, S],
    val connectionSettings: S,
    val ftpClient: () => FtpClient
) extends GraphStageLogic(shape) {

  protected[this] implicit val client = ftpClient()
  protected[this] var handler: Option[ftpLike.Handler] = Option.empty[ftpLike.Handler]
  protected[this] var isConnected: Boolean = false

  override def preStart(): Unit = {
    super.preStart()
    try {
      val tryConnect = ftpLike.connect(connectionSettings)
      if (tryConnect.isSuccess) {
        handler = tryConnect.toOption
        isConnected = true
      } else
        tryConnect.failed.foreach { case NonFatal(t) => throw t }
      doPreStart()
    } catch {
      case NonFatal(t) =>
        disconnect()
        matFailure(t)
        failStage(t)
    }
  }

  override def postStop(): Unit = {
    disconnect()
    matSuccess()
    super.postStop()
  }

  protected[this] def doPreStart(): Unit

  protected[this] def disconnect(): Unit =
    if (isConnected) {
      ftpLike.disconnect(handler.get)
      isConnected = false
    }

  protected[this] def matSuccess(): Boolean

  protected[this] def matFailure(t: Throwable): Boolean

}
