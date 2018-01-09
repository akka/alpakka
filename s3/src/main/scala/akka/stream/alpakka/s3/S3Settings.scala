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
    val s3Config = config.getConfig(configurationPrefix)

    val bufferType = s3Config.getString("buffer") match {
      case "memory" =>
        MemoryBufferType

      case "disk" =>
        val diskBufferPath = s3Config.getString("disk-buffer-path")
        DiskBufferType(Paths.get(diskBufferPath))

      case other =>
        throw new IllegalArgumentException(s"Buffer type must be 'memory' or 'disk'. Got: [$other]")
    }

    val maybeProxy = for {
      host ← Try(s3Config.getString("proxy.host")).toOption if host.nonEmpty
    } yield {
      Proxy(
        host,
        s3Config.getInt("proxy.port"),
        Uri.httpScheme(s3Config.getBoolean("proxy.secure"))
      )
    }

    val pathStyleAccess = s3Config.getBoolean("path-style-access")

    val regionProvider = {
      val regionProviderPath = "aws.region.provider"

      val staticRegionProvider = new AwsRegionProvider {
        lazy val getRegion: String = s3Config.getString("aws.region.default-region")
      }

      if (s3Config.hasPath(regionProviderPath)) {
        s3Config.getString(regionProviderPath) match {
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
      val credProviderPath = "aws.credentials.provider"

      if (s3Config.hasPath(credProviderPath)) {
        s3Config.getString(credProviderPath) match {
          case "default" ⇒
            DefaultAWSCredentialsProviderChain.getInstance()

          case "static" ⇒
            val aki = s3Config.getString("aws.credentials.access-key-id")
            val sak = s3Config.getString("aws.credentials.secret-access-key")
            val tokenPath = "aws.credentials.token"
            val creds = if (s3Config.hasPath(tokenPath)) {
              new BasicSessionCredentials(aki, sak, s3Config.getString(tokenPath))
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
        val deprecatedAccessKeyPath: String = "aws.access-key-id"
        val deprecatedSecretKeyPath: String = "aws.secret-access-key"
        val hasOldCredentials: Boolean = {
          s3Config.hasPath(deprecatedAccessKeyPath) && s3Config.hasPath(deprecatedSecretKeyPath)
        }
        if (hasOldCredentials) {
          new AWSStaticCredentialsProvider(
            new BasicAWSCredentials(
              s3Config.getString(deprecatedAccessKeyPath),
              s3Config.getString(deprecatedSecretKeyPath)
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
