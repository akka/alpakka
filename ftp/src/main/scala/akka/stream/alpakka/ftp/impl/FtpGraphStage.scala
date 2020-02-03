/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.ftp.impl

import akka.stream.alpakka.ftp.RemoteFileSettings
import akka.stream.impl.Stages.DefaultAttributes.IODispatcher
import akka.stream.stage.GraphStage
import akka.stream.{Attributes, Outlet, SourceShape}

trait FtpGraphStage[FtpClient, S <: RemoteFileSettings, T] extends GraphStage[SourceShape[T]] {
  def name: String

  def basePath: String

  def connectionSettings: S

  def ftpClient: () => FtpClient

  val shape: SourceShape[T] = SourceShape(Outlet[T](s"$name.out"))

  val out: Outlet[T] = shape.outlets.head.asInstanceOf[Outlet[T]]

  override def initialAttributes: Attributes =
    super.initialAttributes and Attributes.name(name) and IODispatcher
}
