/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.s3

import java.nio.file.{Path, Paths}
import scala.util.Try
import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri
import com.amazonaws.auth._
import com.typesafe.config.Config

final case class Proxy(host: String, port: Int, scheme: String)

final case class S3Settings(bufferType: BufferType,
                            proxy: Option[Proxy],
                            credentialsProvider: AWSCredentialsProvider,
                            s3Region: String,
                            pathStyleAccess: Boolean) {

  override def toString: String =
    s"""S3Settings(
       |$bufferType,
       |$proxy,
       |${credentialsProvider.getClass.getSimpleName},
       |$s3Region,
       |$pathStyleAccess)""".stripMargin
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
case object DiskBufferType {
  def create(path: Path): DiskBufferType = DiskBufferType(path)
}

object S3Settings {

  /**
   * Scala API: Creates [[S3Settings]] from the [[Config]] attached to an [[ActorSystem]].
   */
  def apply()(implicit system: ActorSystem): S3Settings = apply(system.settings.config)

  /**
   * Scala API: Creates [[S3Settings]] from a [[Config]] object.
   */
  def apply(config: Config): S3Settings = {
    val bufferType = config.getString("akka.stream.alpakka.s3.buffer") match {
      case "memory" =>
        MemoryBufferType

      case "disk" =>
        val diskBufferPath = config.getString("akka.stream.alpakka.s3.disk-buffer-path")
        DiskBufferType(Paths.get(diskBufferPath))

      case other =>
        throw new IllegalArgumentException(s"Buffer type must be 'memory' or 'disk'. Got: [$other]")
    }

    val maybeProxy = for {
      host ← Try(config.getString("akka.stream.alpakka.s3.proxy.host")).toOption if host.nonEmpty
    } yield {
      Proxy(
        host,
        config.getInt("akka.stream.alpakka.s3.proxy.port"),
        Uri.httpScheme(config.getBoolean("akka.stream.alpakka.s3.proxy.secure"))
      )
    }

    val s3region = config.getString("akka.stream.alpakka.s3.aws.default-region")
    val pathStyleAccess = config.getBoolean("akka.stream.alpakka.s3.path-style-access")

    val credentialsProvider = {
      val credProviderPath = "akka.stream.alpakka.s3.aws.credentials.provider"

      if (config.hasPath(credProviderPath)) {
        config.getString(credProviderPath) match {
          case "default" ⇒
            DefaultAWSCredentialsProviderChain.getInstance()

          case "static" ⇒
            val aki = config.getString("akka.stream.alpakka.s3.aws.credentials.access-key-id")
            val sak = config.getString("akka.stream.alpakka.s3.aws.credentials.secret-access-key")
            val tokenPath = "akka.stream.alpakka.s3.aws.credentials.token"
            val creds = if (config.hasPath(tokenPath)) {
              new BasicSessionCredentials(aki, sak, config.getString(tokenPath))
            } else {
              new BasicAWSCredentials(aki, sak)
            }
            new AWSStaticCredentialsProvider(creds)

          case "anon" ⇒
            new AWSStaticCredentialsProvider(new AnonymousAWSCredentials())

          case _ ⇒
            DefaultAWSCredentialsProviderChain.getInstance()
        }
      } else {
        val deprecatedAccessKeyPath: String = "akka.stream.alpakka.s3.aws.access-key-id"
        val deprecatedSecretKeyPath: String = "akka.stream.alpakka.s3.aws.secret-access-key"
        val hasOldCredentials: Boolean = {
          config.hasPath(deprecatedAccessKeyPath) && config.hasPath(deprecatedSecretKeyPath)
        }
        if (hasOldCredentials) {
          new AWSStaticCredentialsProvider(
            new BasicAWSCredentials(
              config.getString(deprecatedAccessKeyPath),
              config.getString(deprecatedSecretKeyPath)
            )
          )
        } else {
          DefaultAWSCredentialsProviderChain.getInstance()
        }
      }
    }

    new S3Settings(
      bufferType = bufferType,
      proxy = maybeProxy,
      credentialsProvider = credentialsProvider,
      s3Region = s3region,
      pathStyleAccess = pathStyleAccess
    )
  }

  /**
   * Java API: Creates [[S3Settings]] from the [[Config]] attached to an [[ActorSystem]].
   */
  def create(system: ActorSystem): S3Settings = apply()(system)

  /**
   * Java API: Creates [[S3Settings]] from a [[Config]].
   */
  def create(config: Config) = apply(config)
}
