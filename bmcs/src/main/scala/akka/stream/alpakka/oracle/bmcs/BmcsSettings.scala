/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.oracle.bmcs

import java.nio.file.{Path, Paths}

import akka.actor.ActorSystem
import com.typesafe.config.Config

final case class Proxy(host: String, port: Int, scheme: String)

final case class BmcsSettings(bufferType: BufferType, proxy: Option[Proxy], region: String, namespace: String) {

  override def toString: String =
    s"BmcsSettings($bufferType,$proxy,$region,$namespace)"
}

sealed trait BufferType {
  def path: Option[Path]
}

case object MemoryBufferType extends BufferType {
  def getInstance: BufferType = MemoryBufferType

  override def path: Option[Path] = None
}

case class DiskBufferType(filePath: Path) extends BufferType {
  override val path: Option[Path] = Some(filePath).filterNot(_.toString.isEmpty)
}

object DiskBufferType {
  def create(filePath: Path): DiskBufferType = DiskBufferType(filePath)
}

object BmcsSettings {

  def apply(region: String, namespace: String, bufferType: BufferType = MemoryBufferType)(
      implicit config: Config
  ): BmcsSettings =
    apply(config).copy(region = region, namespace = namespace, bufferType = bufferType)

  /**
   * Scala API: Creates [[BmcsSettings]] from the [[com.typesafe.config.Config]] attached to an [[akka.actor.ActorSystem]].
   */
  def apply()(implicit system: ActorSystem): BmcsSettings = apply(system.settings.config)

  def apply(config: Config): BmcsSettings = {

    val bufferType = config.getString("akka.stream.alpakka.bmcs.buffer") match {
      case "memory" => MemoryBufferType
      case "disk" =>
        val diskBufferPath = config.getString("akka.stream.alpakka.bmcs.disk-buffer-path")
        DiskBufferType(Paths.get(diskBufferPath))
      case _ => throw new IllegalArgumentException("Buffer type must be 'memory' or 'disk'")
    }
//    val diskBufferPath = config.getString("akka.stream.alpakka.bmcs.disk-buffer-path")
    val proxy = {
      if (config.getString("akka.stream.alpakka.bmcs.proxy.host") != "") {
        val scheme = if (config.getBoolean("akka.stream.alpakka.bmcs.proxy.secure")) "https" else "http"
        Some(
          Proxy(config.getString("akka.stream.alpakka.bmcs.proxy.host"),
                config.getInt("akka.stream.alpakka.bmcs.proxy.port"),
                scheme)
        )
      } else None
    }
    val region = config.getString("akka.stream.alpakka.bmcs.default-region")

    val namespace = config.getString("akka.stream.alpakka.bmcs.default-namespace")

    BmcsSettings(bufferType, proxy, region, namespace)
  }

  /**
   * Java API: Creates [[BmcsSettings]] from the [[Config]] attached to an [[ActorSystem]].
   */
  def create(system: ActorSystem): BmcsSettings = apply()(system)

  /**
   * Java API: Creates [[BmcsSettings]] from a [[Config]].
   */
  def create(config: Config) = apply(config)

  /**
   * Java API: Creates [[BmcsSettings]] from a [[Config]] and overrides region namespace and bufferType.
   */
  def create(region: String, namespace: String, bufferType: BufferType, config: Config) =
    apply(region, namespace, bufferType)(config)

  /**
   * Java API: Creates [[BmcsSettings]] with no proxy. Should be used as default in java.
   *
   */
  def create(region: String, namespace: String, bufferType: BufferType) =
    BmcsSettings(region = region, namespace = namespace, bufferType = bufferType, proxy = None)

}
