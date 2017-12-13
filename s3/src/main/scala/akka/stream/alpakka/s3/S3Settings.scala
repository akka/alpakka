/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.s3

import java.nio.file.{Path, Paths}

import scala.util.Try

import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri
import com.amazonaws.auth._
import com.amazonaws.regions.{AwsRegionProvider, DefaultAwsRegionProviderChain}
import com.typesafe.config.Config

final case class Proxy(host: String, port: Int, scheme: String)

final case class S3Settings(bufferType: BufferType,
                            proxy: Option[Proxy],
                            credentialsProvider: AWSCredentialsProvider,
                            s3RegionProvider: AwsRegionProvider,
                            pathStyleAccess: Boolean) {

  override def toString: String =
    s"""S3Settings(
       |$bufferType,
       |$proxy,
       |${credentialsProvider.getClass.getSimpleName},
       |${s3RegionProvider.getClass.getSimpleName},
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
   * Scala API: Creates [[S3Settings]] from the [[Config]] attached to an [[ActorSystem]].
   */
  def apply(configurationPrefix: String)(implicit system: ActorSystem): S3Settings =
    apply(system.settings.config, configurationPrefix)

  /**
   * Scala API: Creates [[S3Settings]] from a [[Config]] object.
   */
  def apply(config: Config, configurationPrefix: String = "akka.stream.alpakka.s3"): S3Settings = {
    val bufferType = config.getString(s"$configurationPrefix.buffer") match {
      case "memory" =>
        MemoryBufferType

      case "disk" =>
        val diskBufferPath = config.getString(s"$configurationPrefix.disk-buffer-path")
        DiskBufferType(Paths.get(diskBufferPath))

      case other =>
        throw new IllegalArgumentException(s"Buffer type must be 'memory' or 'disk'. Got: [$other]")
    }

    val maybeProxy = for {
      host ← Try(config.getString(s"$configurationPrefix.proxy.host")).toOption if host.nonEmpty
    } yield {
      Proxy(
        host,
        config.getInt(s"$configurationPrefix.proxy.port"),
        Uri.httpScheme(config.getBoolean(s"$configurationPrefix.proxy.secure"))
      )
    }

    val pathStyleAccess = config.getBoolean(s"$configurationPrefix.path-style-access")

    val regionProvider = {
      val regionProviderPath = s"$configurationPrefix.aws.region.provider"

      val staticRegionProvider = new AwsRegionProvider {
        lazy val getRegion: String = {
          if (config.hasPath(s"$configurationPrefix.aws.region.default-region")) {
            config.getString(s"$configurationPrefix.aws.region.default-region")
          } else {
            config.getString(s"$configurationPrefix.aws.default-region")
          }
        }
      }

      if (config.hasPath(regionProviderPath)) {
        config.getString(regionProviderPath) match {
          case "static" =>
            staticRegionProvider

          case _ =>
            new DefaultAwsRegionProviderChain()
        }
      } else {
        new DefaultAwsRegionProviderChain()
      }
    }

    val credentialsProvider = {
      val credProviderPath = s"$configurationPrefix.aws.credentials.provider"

      if (config.hasPath(credProviderPath)) {
        config.getString(credProviderPath) match {
          case "default" ⇒
            DefaultAWSCredentialsProviderChain.getInstance()

          case "static" ⇒
            val aki = config.getString(s"$configurationPrefix.aws.credentials.access-key-id")
            val sak = config.getString(s"$configurationPrefix.aws.credentials.secret-access-key")
            val tokenPath = s"$configurationPrefix.aws.credentials.token"
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
        val deprecatedAccessKeyPath: String = s"$configurationPrefix.aws.access-key-id"
        val deprecatedSecretKeyPath: String = s"$configurationPrefix.aws.secret-access-key"
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
      s3RegionProvider = regionProvider,
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
