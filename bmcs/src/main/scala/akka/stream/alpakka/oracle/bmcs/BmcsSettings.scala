/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.oracle.bmcs

import akka.actor.ActorSystem
import akka.stream.alpakka.oracle.bmcs.auth.BmcsCredentials
import com.typesafe.config.Config

final case class Proxy(host: String, port: Int, scheme: String)

final case class BmcsSettings(bufferType: BufferType,
                              diskBufferPath: String,
                              proxy: Option[Proxy],
                              region: String,
                              namespace: String) {

  override def toString: String =
    s"BmcsSettings($bufferType$diskBufferPath,$proxy,$region)"
}

sealed trait BufferType

case object MemoryBufferType extends BufferType {
  def getInstance: BufferType = MemoryBufferType
}

case object DiskBufferType extends BufferType {
  def getInstance: BufferType = DiskBufferType
}

object BmcsSettings {

  /**
   * Scala API: Creates [[BmcsSettings]] from the [[com.typesafe.config.Config]] attached to an [[akka.actor.ActorSystem]].
   */
  def apply()(implicit system: ActorSystem): BmcsSettings = apply(system.settings.config)

  def apply(config: Config): BmcsSettings = {

    val bufferType = config.getString("akka.stream.alpakka.bmcs.buffer") match {
      case "memory" => MemoryBufferType
      case "disk" => DiskBufferType
      case _ => throw new IllegalArgumentException("Buffer type must be 'memory' or 'disk'")
    }
    val diskBufferPath = config.getString("akka.stream.alpakka.bmcs.disk-buffer-path")
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

    BmcsSettings(bufferType, diskBufferPath, proxy, region, namespace)
  }

  /**
   * Java API: Creates [[BmcsSettings]] from the [[Config]] attached to an [[ActorSystem]].
   */
  def create(system: ActorSystem): BmcsSettings = apply()(system)

  /**
   * Java API: Creates [[BmcsSettings]] from a [[Config]].
   */
  def create(config: Config) = apply(config)

}
