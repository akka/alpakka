/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google.auth

import akka.actor.ClassicActorSystemProvider
import akka.annotation.DoNotInherit
import akka.event.Logging
import akka.http.scaladsl.model.headers.HttpCredentials
import akka.stream.alpakka.google.RequestSettings
import akka.util.JavaDurationConverters._
import com.google.auth.{Credentials => GoogleCredentials}
import com.typesafe.config.Config

import java.util.concurrent.Executor
import scala.collection.immutable.ListMap
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.control.NonFatal

object Credentials {

  /**
   * Creates [[Credentials]] to access Google APIs from a given configuration.
   * Assume that construction is "resource-heavy" (e.g. spawns actors) so prefer to cache and reuse.
   */
  def apply(c: Config)(implicit system: ClassicActorSystemProvider): Credentials = c.getString("provider") match {
    case "application-default" =>
      try {
        parseServiceAccount(c)
      } catch {
        case NonFatal(ex1) =>
          try {
            parseComputeEngine(c)
          } catch {
            case NonFatal(ex2) =>
              val log = Logging(system.classicSystem, getClass)
              log.warning("Unable to find Application Default Credentials for Google APIs")
              log.warning("Service account: {}", ex1.getMessage)
              log.warning("Compute Engine: {}", ex2.getMessage)
              parseNone(c) // TODO Once credentials are guaranteed to be managed centrally we can throw an error instead
          }
      }
    case "service-account" => parseServiceAccount(c)
    case "compute-engine" => parseComputeEngine(c)
    case "none" => parseNone(c)
  }

  private def parseServiceAccount(c: Config)(implicit system: ClassicActorSystemProvider) =
    ServiceAccountCredentials(c.getConfig("service-account"))

  private def parseComputeEngine(c: Config)(implicit system: ClassicActorSystemProvider) =
    Await.result(ComputeEngineCredentials(), c.getDuration("compute-engine.timeout").asScala)

  private def parseNone(c: Config) = NoCredentials(c.getConfig("none"))

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

  private[google] def projectId: String

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
}
