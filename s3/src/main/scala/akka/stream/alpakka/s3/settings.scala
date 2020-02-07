/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.s3

import java.nio.file.{Path, Paths}
import java.util.Objects
import java.util.concurrent.TimeUnit

import scala.util.Try
import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri
import software.amazon.awssdk.auth.credentials._
import software.amazon.awssdk.regions.providers._
import com.typesafe.config.Config
import software.amazon.awssdk.regions.Region

import scala.compat.java8.OptionConverters._
import scala.concurrent.duration._

final class Proxy private (
    val host: String,
    val port: Int,
    val scheme: String
) {

  /** Java API */
  def getHost: String = host

  /** Java API */
  def getPort: Int = port

  /** Java API */
  def getScheme: String = scheme

  def withHost(value: String): Proxy = copy(host = value)
  def withPort(value: Int): Proxy = copy(port = value)
  def withScheme(value: String): Proxy = copy(scheme = value)

  private def copy(host: String = host, port: Int = port, scheme: String = scheme): Proxy =
    new Proxy(host = host, port = port, scheme = scheme)

  override def toString =
    "Proxy(" +
    s"host=$host," +
    s"port=$port," +
    s"scheme=$scheme" +
    ")"

  override def equals(other: Any): Boolean = other match {
    case that: Proxy =>
      Objects.equals(this.host, that.host) &&
      Objects.equals(this.port, that.port) &&
      Objects.equals(this.scheme, that.scheme)
    case _ => false
  }

  override def hashCode(): Int =
    Objects.hash(host, Int.box(port), scheme)
}

object Proxy {

  /** Scala API */
  def apply(host: String, port: Int, scheme: String): Proxy =
    new Proxy(host, port, scheme)

  /** Java API */
  def create(host: String, port: Int, scheme: String): Proxy =
    apply(host, port, scheme)
}

final class ForwardProxyCredentials private (val username: String, val password: String) {

  /** Java API */
  def getUsername: String = username

  /** Java API */
  def getPassword: String = password

  def withUsername(username: String) = copy(username = username)
  def withPassword(password: String) = copy(password = password)

  private def copy(username: String = username, password: String = password) =
    new ForwardProxyCredentials(username, password)

  override def toString =
    "ForwardProxyCredentials(" +
    s"username=$username," +
    s"password=******" +
    ")"

  override def equals(other: Any): Boolean = other match {
    case that: ForwardProxyCredentials =>
      Objects.equals(this.username, that.username) &&
      Objects.equals(this.password, that.password)
    case _ => false
  }

  override def hashCode(): Int =
    Objects.hash(username, password)

}

object ForwardProxyCredentials {

  /** Scala API */
  def apply(username: String, password: String): ForwardProxyCredentials =
    new ForwardProxyCredentials(username, password)

  /** Java API */
  def create(username: String, password: String): ForwardProxyCredentials =
    apply(username, password)

}

final class ForwardProxy private (val host: String, val port: Int, val credentials: Option[ForwardProxyCredentials]) {

  /** Java API */
  def getHost: String = host

  /** Java API */
  def getPort: Int = port

  /** Java API */
  def getCredentials: java.util.Optional[ForwardProxyCredentials] = credentials.asJava

  def withHost(host: String) = copy(host = host)
  def withPort(port: Int) = copy(port = port)
  def withCredentials(credentials: ForwardProxyCredentials) = copy(credentials = Option(credentials))

  private def copy(host: String = host, port: Int = port, credentials: Option[ForwardProxyCredentials] = credentials) =
    new ForwardProxy(host, port, credentials)

  override def toString =
    "ForwardProxy(" +
    s"host=$host," +
    s"port=$port," +
    s"credentials=$credentials" +
    ")"

  override def equals(other: Any): Boolean = other match {
    case that: ForwardProxy =>
      Objects.equals(this.host, that.host) &&
      Objects.equals(this.port, that.port) &&
      Objects.equals(this.credentials, that.credentials)
    case _ => false
  }

  override def hashCode(): Int =
    Objects.hash(host, Int.box(port), credentials)
}

object ForwardProxy {

  /** Scala API */
  def apply(host: String, port: Int, credentials: Option[ForwardProxyCredentials]) =
    new ForwardProxy(host, port, credentials)

  /** Java API */
  def create(host: String, port: Int, credentials: Option[ForwardProxyCredentials]) =
    apply(host, port, credentials)

}

sealed abstract class ApiVersion
object ApiVersion {
  sealed abstract class ListBucketVersion1 extends ApiVersion
  case object ListBucketVersion1 extends ListBucketVersion1

  /** Java Api */
  def getListBucketVersion1: ListBucketVersion1 = ListBucketVersion1

  sealed abstract class ListBucketVersion2 extends ApiVersion
  case object ListBucketVersion2 extends ListBucketVersion2

  /** Java Api */
  def getListBucketVersion2: ListBucketVersion2 = ListBucketVersion2
}

final class S3Settings private (
    val bufferType: BufferType,
    val credentialsProvider: AwsCredentialsProvider,
    val s3RegionProvider: AwsRegionProvider,
    val pathStyleAccess: Boolean,
    val endpointUrl: Option[String],
    val listBucketApiVersion: ApiVersion,
    val forwardProxy: Option[ForwardProxy],
    val validateObjectKey: Boolean,
    val retrySettings: RetrySettings
) {

  @deprecated("Please use endpointUrl instead", since = "1.0.1") val proxy: Option[Proxy] = None

  /** Java API */
  def getBufferType: BufferType = bufferType

  /** Java API */
  @deprecated("Please use endpointUrl instead", since = "1.0.1")
  def getProxy: java.util.Optional[Proxy] = proxy.asJava

  /** Java API */
  def getCredentialsProvider: AwsCredentialsProvider = credentialsProvider

  /** Java API */
  def getS3RegionProvider: AwsRegionProvider = s3RegionProvider

  /** Java API */
  def isPathStyleAccess: Boolean = pathStyleAccess

  /** Java API */
  def getEndpointUrl: java.util.Optional[String] = endpointUrl.asJava

  /** Java API */
  def getListBucketApiVersion: ApiVersion = listBucketApiVersion

  /** Java API */
  def getForwardProxy: java.util.Optional[ForwardProxy] = forwardProxy.asJava

  def withBufferType(value: BufferType): S3Settings = copy(bufferType = value)

  @deprecated("Please use endpointUrl instead", since = "1.0.1")
  def withProxy(value: Proxy): S3Settings = copy(endpointUrl = Some(s"${value.scheme}://${value.host}:${value.port}"))

  def withCredentialsProvider(value: AwsCredentialsProvider): S3Settings =
    copy(credentialsProvider = value)
  def withS3RegionProvider(value: AwsRegionProvider): S3Settings = copy(s3RegionProvider = value)

  @deprecated(
    "AWS S3 is going to retire path-style access https://aws.amazon.com/blogs/aws/amazon-s3-path-deprecation-plan-the-rest-of-the-story/",
    since = "2.0.0"
  )
  def withPathStyleAccess(value: Boolean): S3Settings =
    if (pathStyleAccess == value) this else copy(pathStyleAccess = value)
  def withEndpointUrl(value: String): S3Settings = copy(endpointUrl = Option(value))
  def withListBucketApiVersion(value: ApiVersion): S3Settings =
    copy(listBucketApiVersion = value)
  def withForwardProxy(value: ForwardProxy): S3Settings =
    copy(forwardProxy = Option(value))
  def withValidateObjectKey(value: Boolean): S3Settings =
    if (validateObjectKey == value) this else copy(validateObjectKey = value)

  def withRetrySettings(value: RetrySettings): S3Settings = copy(retrySettings = value)

  private def copy(
      bufferType: BufferType = bufferType,
      credentialsProvider: AwsCredentialsProvider = credentialsProvider,
      s3RegionProvider: AwsRegionProvider = s3RegionProvider,
      pathStyleAccess: Boolean = pathStyleAccess,
      endpointUrl: Option[String] = endpointUrl,
      listBucketApiVersion: ApiVersion = listBucketApiVersion,
      forwardProxy: Option[ForwardProxy] = forwardProxy,
      validateObjectKey: Boolean = validateObjectKey,
      retrySettings: RetrySettings = retrySettings
  ): S3Settings = new S3Settings(
    bufferType = bufferType,
    credentialsProvider = credentialsProvider,
    s3RegionProvider = s3RegionProvider,
    pathStyleAccess = pathStyleAccess,
    endpointUrl = endpointUrl,
    listBucketApiVersion = listBucketApiVersion,
    forwardProxy = forwardProxy,
    validateObjectKey,
    retrySettings
  )

  override def toString =
    "S3Settings(" +
    s"bufferType=$bufferType," +
    s"credentialsProvider=$credentialsProvider," +
    s"s3RegionProvider=$s3RegionProvider," +
    s"pathStyleAccess=$pathStyleAccess," +
    s"endpointUrl=$endpointUrl," +
    s"listBucketApiVersion=$listBucketApiVersion," +
    s"forwardProxy=$forwardProxy," +
    s"validateObjectKey=$validateObjectKey" +
    ")"

  override def equals(other: Any): Boolean = other match {
    case that: S3Settings =>
      java.util.Objects.equals(this.bufferType, that.bufferType) &&
      Objects.equals(this.credentialsProvider, that.credentialsProvider) &&
      Objects.equals(this.s3RegionProvider, that.s3RegionProvider) &&
      Objects.equals(this.pathStyleAccess, that.pathStyleAccess) &&
      Objects.equals(this.endpointUrl, that.endpointUrl) &&
      Objects.equals(this.listBucketApiVersion, that.listBucketApiVersion) &&
      Objects.equals(this.forwardProxy, that.forwardProxy) &&
      this.validateObjectKey == that.validateObjectKey
    case _ => false
  }

  override def hashCode(): Int =
    Objects.hash(
      bufferType,
      credentialsProvider,
      s3RegionProvider,
      Boolean.box(pathStyleAccess),
      endpointUrl,
      listBucketApiVersion,
      forwardProxy,
      Boolean.box(validateObjectKey)
    )
}

object S3Settings {
  val ConfigPath = "alpakka.s3"

  /**
   * Reads from the given config.
   */
  def apply(c: Config): S3Settings = {

    val bufferType = c.getString("buffer") match {
      case "memory" =>
        MemoryBufferType

      case "disk" =>
        val diskBufferPath = c.getString("disk-buffer-path")
        DiskBufferType(Paths.get(diskBufferPath))

      case other =>
        throw new IllegalArgumentException(s"Buffer type must be 'memory' or 'disk'. Got: [$other]")
    }

    val maybeProxy = for {
      host ← Try(c.getString("proxy.host")).toOption if host.nonEmpty
    } yield {
      Proxy(
        host,
        c.getInt("proxy.port"),
        Uri.httpScheme(c.getBoolean("proxy.secure"))
      )
    }

    val maybeForwardProxy = if (c.hasPath("forward-proxy")) {
      val maybeCredentials = if (c.hasPath("forward-proxy.credentials")) {
        Option(
          ForwardProxyCredentials(c.getString("forward-proxy.credentials.username"),
                                  c.getString("forward-proxy.credentials.password"))
        )
      } else None

      Option(ForwardProxy(c.getString("forward-proxy.host"), c.getInt("forward-proxy.port"), maybeCredentials))
    } else None

    val pathStyleAccess = c.getBoolean("path-style-access")

    val endpointUrl = if (c.hasPath("endpoint-url")) {
      Option(c.getString("endpoint-url"))
    } else {
      None
    }.orElse(maybeProxy.map(p => s"${p.scheme}://${p.host}:${p.port}"))

    val regionProvider = {
      val regionProviderPath = "aws.region.provider"

      val staticRegionProvider = new AwsRegionProvider {
        lazy val getRegion: Region = Region.of(c.getString("aws.region.default-region"))
      }

      if (c.hasPath(regionProviderPath)) {
        c.getString(regionProviderPath) match {
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

      if (c.hasPath(credProviderPath)) {
        c.getString(credProviderPath) match {
          case "default" ⇒
            DefaultCredentialsProvider.create()

          case "static" ⇒
            val aki = c.getString("aws.credentials.access-key-id")
            val sak = c.getString("aws.credentials.secret-access-key")
            val tokenPath = "aws.credentials.token"
            val creds = if (c.hasPath(tokenPath)) {
              AwsSessionCredentials.create(aki, sak, c.getString(tokenPath))
            } else {
              AwsBasicCredentials.create(aki, sak)
            }
            StaticCredentialsProvider.create(creds)

          case "anon" ⇒
            AnonymousCredentialsProvider.create()

          case _ ⇒
            DefaultCredentialsProvider.create()
        }
      } else {
        DefaultCredentialsProvider.create()
      }
    }

    val apiVersion = Try(c.getInt("list-bucket-api-version") match {
      case 1 => ApiVersion.ListBucketVersion1
      case 2 => ApiVersion.ListBucketVersion2
    }).getOrElse(ApiVersion.ListBucketVersion2)
    val validateObjectKey = c.getBoolean("validate-object-key")

    val retryConfig = c.getConfig("retry-settings")
    val retrySettings = RetrySettings(
      retryConfig.getInt("max-retries-per-chunk"),
      FiniteDuration(retryConfig.getDuration("min-backoff").toNanos, TimeUnit.NANOSECONDS),
      FiniteDuration(retryConfig.getDuration("max-backoff").toNanos, TimeUnit.NANOSECONDS),
      retryConfig.getDouble("random-factor")
    )

    new S3Settings(
      bufferType = bufferType,
      credentialsProvider = credentialsProvider,
      s3RegionProvider = regionProvider,
      pathStyleAccess = pathStyleAccess,
      endpointUrl = endpointUrl,
      listBucketApiVersion = apiVersion,
      forwardProxy = maybeForwardProxy,
      validateObjectKey,
      retrySettings
    )
  }

  /**
   * Java API: Reads from the given config.
   */
  def create(c: Config): S3Settings = apply(c)

  /** Scala API */
  @deprecated("Please use the other factory method that takes only mandatory attributes", since = "1.0.1")
  def apply(
      bufferType: BufferType,
      proxy: Option[Proxy],
      credentialsProvider: AwsCredentialsProvider,
      s3RegionProvider: AwsRegionProvider,
      pathStyleAccess: Boolean,
      endpointUrl: Option[String],
      listBucketApiVersion: ApiVersion
  ): S3Settings = new S3Settings(
    bufferType,
    credentialsProvider,
    s3RegionProvider,
    pathStyleAccess,
    endpointUrl,
    listBucketApiVersion,
    None,
    validateObjectKey = true,
    RetrySettings.default
  )

  /** Scala API */
  def apply(
      bufferType: BufferType,
      credentialsProvider: AwsCredentialsProvider,
      s3RegionProvider: AwsRegionProvider,
      listBucketApiVersion: ApiVersion
  ): S3Settings = new S3Settings(
    bufferType,
    credentialsProvider,
    s3RegionProvider,
    false,
    None,
    listBucketApiVersion,
    None,
    validateObjectKey = true,
    RetrySettings.default
  )

  /** Java API */
  @deprecated("Please use the other factory method that takes only mandatory attributes", since = "1.0.1")
  def create(
      bufferType: BufferType,
      proxy: java.util.Optional[Proxy],
      credentialsProvider: AwsCredentialsProvider,
      s3RegionProvider: AwsRegionProvider,
      pathStyleAccess: Boolean,
      endpointUrl: java.util.Optional[String],
      listBucketApiVersion: ApiVersion
  ): S3Settings = apply(
    bufferType,
    proxy.asScala,
    credentialsProvider,
    s3RegionProvider,
    pathStyleAccess,
    endpointUrl.asScala,
    listBucketApiVersion
  )

  /** Java API */
  def create(
      bufferType: BufferType,
      credentialsProvider: AwsCredentialsProvider,
      s3RegionProvider: AwsRegionProvider,
      listBucketApiVersion: ApiVersion
  ): S3Settings = apply(
    bufferType,
    credentialsProvider,
    s3RegionProvider,
    listBucketApiVersion
  )

  /**
   * Scala API: Creates [[S3Settings]] from the [[com.typesafe.config.Config Config]] attached to an [[akka.actor.ActorSystem]].
   */
  def apply()(implicit system: ActorSystem): S3Settings = apply(system.settings.config.getConfig(ConfigPath))

  /**
   * Java API: Creates [[S3Settings]] from the [[com.typesafe.config.Config Config]] attached to an [[akka.actor.ActorSystem]].
   */
  def create(system: ActorSystem): S3Settings = apply()(system)
}

sealed trait BufferType {
  def path: Option[Path]

  /** Java API */
  def getPath: java.util.Optional[Path] = path.asJava
}

case object MemoryBufferType extends BufferType {
  def getInstance: BufferType = MemoryBufferType
  override def path: Option[Path] = None
}

final class DiskBufferType private (filePath: Path) extends BufferType {
  override val path: Option[Path] = Some(filePath).filterNot(_.toString.isEmpty)
}
case object DiskBufferType {
  def apply(path: Path): DiskBufferType = new DiskBufferType(path)

  /** Java API */
  def create(path: Path): DiskBufferType = DiskBufferType(path)
}

final case class RetrySettings(maxRetriesPerChunk: Int,
                               minBackoff: FiniteDuration,
                               maxBackoff: FiniteDuration,
                               randomFactor: Double)

object RetrySettings {
  val default: RetrySettings = RetrySettings(3, 200.milliseconds, 10.seconds, 0.0)
}
