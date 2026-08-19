/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.google.auth

import akka.actor.ClassicActorSystemProvider
import akka.annotation.DoNotInherit
import akka.event.Logging
import akka.http.scaladsl.model.headers.HttpCredentials
import akka.stream.alpakka.google.RequestSettings
import com.google.auth.{Credentials => GoogleCredentials}
import com.typesafe.config.Config

import java.util.concurrent.Executor
import scala.annotation.tailrec
import scala.collection.immutable.ListMap
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.jdk.DurationConverters._
import scala.util.control.NonFatal

object Credentials {

  /**
   * Creates [[Credentials]] to access Google APIs from a given configuration.
   * Assume that construction is "resource-heavy" (e.g. spawns actors) so prefer to cache and reuse.
   */
  def apply(c: Config)(implicit system: ClassicActorSystemProvider): Credentials = c.getString("provider") match {
    case "application-default" =>
      val log = Logging(system.classicSystem, classOf[Credentials])
      try {
        val creds = parseApplicationDefault(c)
        log.info("Using service account credentials")
        creds
      } catch {
        case NonFatal(ex1) =>
          try {
            val creds = parseComputeEngine(c)
            log.info("Using Compute Engine credentials")
            creds
          } catch {
            case NonFatal(ex2) =>
              log.warning(
                "Unable to find Application Default Credentials for Google APIs. Falling back to the `none` " +
                "credentials provider: requests to Google APIs will be made without valid credentials and will be " +
                "rejected (typically with 401 or 403) unless the resource is publicly accessible. Configure " +
                "`alpakka.google.credentials.provider` explicitly if this fallback is not intended. " +
                "Application default credentials failed with [{}]. " +
                "Compute Engine credentials failed with [{}]; a timeout here means the metadata server at [{}] " +
                "did not respond within `alpakka.google.credentials.compute-engine.timeout`, which indicates a " +
                "connectivity problem (not running on Google Cloud, or the request being intercepted or blocked by " +
                "a proxy or service mesh) rather than a problem with the credentials themselves.",
                describe(ex1),
                describe(ex2),
                GoogleComputeMetadata.metadataUrl
              )
              parseNone(c) // TODO Once credentials are guaranteed to be managed centrally we can throw an error instead
          }
      }
    case "service-account" => parseServiceAccount(c)
    case "compute-engine" => parseComputeEngine(c)
    case "user-access" => parseUserAccess(c)
    case "none" => parseNone(c)
  }

  private def parseServiceAccount(c: Config)(implicit system: ClassicActorSystemProvider) =
    ServiceAccountCredentials(c.getConfig("service-account"))

  private def parseComputeEngine(c: Config)(implicit system: ClassicActorSystemProvider) =
    Await.result(ComputeEngineCredentials(), c.getDuration("compute-engine.timeout").toScala)

  private def parseUserAccess(c: Config)(implicit system: ClassicActorSystemProvider) =
    UserAccessCredentials(c.getConfig("user-access"))

  private def parseApplicationDefault(c: Config)(implicit system: ClassicActorSystemProvider) = {
    val scopes = c.getStringList("scopes").asScala.toSeq
    ApplicationDefaultCredentials(c.getConfig("application-default"), scopes)
  }

  private def parseNone(c: Config) = NoCredentials(c.getConfig("none"))

  /**
   * Renders the message of `ex` together with the messages of its causes, since the cause chain often carries the
   * actual reason (e.g. a connection timeout) while the outermost message is generic.
   */
  private def describe(ex: Throwable): String = {
    @tailrec def loop(ex: Throwable, depth: Int, acc: List[String]): List[String] =
      if ((ex eq null) || depth == 0) acc
      else loop(ex.getCause, depth - 1, s"${ex.getClass.getName}: ${ex.getMessage}" :: acc)
    loop(ex, 5, Nil).reverse.mkString(", caused by ")
  }

  private var _cache: Map[Any, Credentials] = ListMap.empty
  @deprecated("Intended only to help with migration", "3.0.0")
  private[alpakka] def cache(key: Any)(default: => Credentials) =
    _cache.getOrElse(key, {
      val credentials = default
      _cache += (key -> credentials)
      credentials
    })

}

/**
 * Credentials for accessing Google APIs
 */
@DoNotInherit
abstract class Credentials private[auth] () {

  private[google] def projectId: Option[String]

  private[google] def get()(implicit ec: ExecutionContext, settings: RequestSettings): Future[HttpCredentials]

  /**
   * Wraps these credentials as a [[com.google.auth.Credentials]] for interop with Google's Java client libraries.
   * @param ec the [[scala.concurrent.ExecutionContext]] to use for blocking requests if credentials are requested synchronously
   * @param settings additional request settings
   */
  def asGoogle(implicit ec: ExecutionContext, settings: RequestSettings): GoogleCredentials

  /**
   * Java API: Wraps these credentials as a [[com.google.auth.Credentials]] for interop with Google's Java client libraries.
   * @param exec the [[java.util.concurrent.Executor]] to use for blocking requests if credentials are requested synchronously
   * @param settings additional request settings
   */
  final def asGoogle(exec: Executor, settings: RequestSettings): GoogleCredentials =
    asGoogle(ExecutionContext.fromExecutor(exec): ExecutionContext, settings)

  /**
   * Release any resources that this credentials provider is using.
   *
   * This is intended to be called in cases where credentials are instantiated dynamically.
   */
  def close(): Unit = {}
}
