/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.s3

import akka.actor.ActorSystem
import akka.stream.alpakka.s3.auth.AWSCredentials
import com.typesafe.config.Config

final case class Proxy(host: String, port: Int)

final class S3Settings(val bufferType: BufferType,
                       val diskBufferPath: String,
                       val proxy: Option[Proxy],
                       val awsCredentials: AWSCredentials,
                       val s3Region: String,
                       val pathStyleAccess: Boolean) {

  override def toString: String =
    s"S3Settings($bufferType,$diskBufferPath,$proxy,$awsCredentials,$s3Region,$pathStyleAccess)"
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
  def apply(config: Config): S3Settings = new S3Settings(
    bufferType = config.getString("buffer") match {
      case "memory" => MemoryBufferType
      case "disk" => DiskBufferType
      case _ => throw new IllegalArgumentException("Buffer type must be 'memory' or 'disk'")
    },
    diskBufferPath = config.getString("disk-buffer-path"),
    proxy = {
      if (config.getString("proxy.host") != "")
        Some(Proxy(config.getString("proxy.host"), config.getInt("proxy.port")))
      else None
    },
    awsCredentials = AWSCredentials(config.getString("aws.access-key-id"), config.getString("aws.secret-access-key")),
    s3Region = config.getString("aws.default-region"),
    pathStyleAccess = config.getBoolean("path-style-access")
  )
}
