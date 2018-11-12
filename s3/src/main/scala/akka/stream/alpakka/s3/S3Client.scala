/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.s3
import akka.actor.{ActorSystem, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import akka.annotation.InternalApi
import akka.stream.{ActorMaterializer, Materializer}
import akka.stream.alpakka.s3.impl.S3Stream
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.regions.AwsRegionProvider
import com.typesafe.config.ConfigFactory

object S3Client {
  val MinChunkSize: Int = 5242880

  /**
   * Create S3Client from the configuration of the given ActorSystem
   */
  def apply()(implicit system: ActorSystem, mat: Materializer): S3Client =
    new S3Client(S3Settings(system.settings.config))

  /**
   * Create S3Client from the configuration of the given ActorSystem and
   * override credentials provider and region.
   */
  def apply(credentialsProvider: AWSCredentialsProvider, region: String)(implicit system: ActorSystem,
                                                                         mat: Materializer): S3Client =
    apply(
      credentialsProvider,
      new AwsRegionProvider {
        def getRegion: String = region
      }
    )

  /**
   * Create S3Client from the configuration of the given ActorSystem and
   * override credentials provider and region provider.
   */
  def apply(credentialsProvider: AWSCredentialsProvider,
            regionProvider: AwsRegionProvider)(implicit system: ActorSystem, mat: Materializer): S3Client = {
    val settings: S3Settings = S3Settings(system.settings.config).copy(
      credentialsProvider = credentialsProvider,
      s3RegionProvider = regionProvider
    )

    new S3Client(settings)
  }

  def apply(settings: S3Settings)(implicit system: ActorSystem, mat: Materializer) =
    new S3Client(settings)

  def create(system: ActorSystem, mat: Materializer): S3Client =
    new S3Client(S3Settings(ConfigFactory.load()))(system, mat)

  def create(credentialsProvider: AWSCredentialsProvider, region: String)(implicit system: ActorSystem,
                                                                          mat: Materializer): S3Client =
    create(
      credentialsProvider,
      new AwsRegionProvider {
        def getRegion: String = region
      }
    )

  def create(credentialsProvider: AWSCredentialsProvider,
             regionProvider: AwsRegionProvider)(implicit system: ActorSystem, mat: Materializer): S3Client = {
    val settings = S3Settings(ConfigFactory.load()).copy(
      credentialsProvider = credentialsProvider,
      s3RegionProvider = regionProvider
    )

    new S3Client(settings)(system, mat)
  }

  def create(s3Settings: S3Settings)(implicit system: ActorSystem, mat: Materializer): S3Client =
    new S3Client(s3Settings)(system, mat)
}

final class S3Client private (val s3Settings: S3Settings)(implicit val system: ActorSystem, val mat: Materializer) {

  /**
   * Internal Api
   */
  @InternalApi private[s3] val impl = S3Stream(s3Settings)
}

/**
 * Manages one [[S3Client]] per `ActorSystem`.
 */
final class S3ClientExt private (sys: ExtendedActorSystem) extends Extension {
  private[this] val systemMaterializer = ActorMaterializer()(sys)
  implicit val client = S3Client()(sys, systemMaterializer)
}

object S3ClientExt extends ExtensionId[S3ClientExt] with ExtensionIdProvider {
  override def lookup = S3ClientExt
  override def createExtension(system: ExtendedActorSystem) = new S3ClientExt(system)
}
