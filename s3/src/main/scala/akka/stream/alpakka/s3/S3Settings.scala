/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.s3

import akka.actor.ActorSystem
import com.typesafe.config.{ Config, ConfigFactory }

final case class Proxy(host: String, port: Int)

final class S3Settings(val bufferType: BufferType,
                       val diskBufferPath: String,
                       val debugLogging: Boolean,
                       val proxy: Option[Proxy]) {
  override def toString: String = s"S3Settings($bufferType,$diskBufferPath,$debugLogging,$proxy)"
}

sealed trait BufferType
case object MemoryBufferType extends BufferType {
  def getInstance: BufferType = MemoryBufferType
}

case object DiskBufferType extends BufferType {
  def getInstance: BufferType = DiskBufferType
}

object S3Settings {
  def apply(system: ActorSystem): S3Settings =
    apply(system.settings.config.getConfig("akka.stream.alpakka.s3"))

  /**
   * Create [[S3Settings]] from a Config subsection.
   */
  def apply(config: Config): S3Settings = {
    val proxyConfig = if (config.hasPath("proxy")) config.getConfig("proxy") else ConfigFactory.empty()
    new S3Settings(
      bufferType = config.getString("buffer") match {
        case "memory" => MemoryBufferType
        case "disk" => DiskBufferType
        case _ => throw new IllegalArgumentException("Buffer type must be 'memory' or 'disk'")
      },
      diskBufferPath = config.getString("disk-buffer-path"),
      debugLogging = config.getBoolean("debug-logging"),
      proxy =
        if (proxyConfig.isEmpty) None
        else Some(Proxy(proxyConfig.getString("host"), proxyConfig.getInt("port")))
    )
  }
}
