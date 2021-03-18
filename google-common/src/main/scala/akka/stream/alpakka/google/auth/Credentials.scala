/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google.auth

import akka.actor.ClassicActorSystemProvider
import akka.annotation.InternalApi
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.stream.alpakka.google.GoogleSettings
import akka.util.JavaDurationConverters._
import com.google.auth.{Credentials => GoogleCredentials}
import com.typesafe.config.Config

import scala.collection.immutable.ListMap
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.control.NonFatal

@InternalApi
private[alpakka] object Credentials {

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
              system.classicSystem.log.warning("Unable to find application default credentials", ex1, ex2)
              NoCredentials // TODO Once credentials are guaranteed to be managed centrally we can throw an error instead
          }
      }
    case "service-account" => parseServiceAccount(c)
    case "compute-engine" => parseComputeEngine(c)
    case "none" => NoCredentials
  }

  private def parseServiceAccount(c: Config)(implicit system: ClassicActorSystemProvider) =
    ServiceAccountCredentials(c.getConfig("service-account"))

  private def parseComputeEngine(c: Config)(implicit system: ClassicActorSystemProvider) =
    Await.result(ComputeEngineCredentials(), c.getDuration("compute-engine.timeout").asScala)

  private var _cache: Map[Any, Credentials] = ListMap.empty
  @deprecated("Intended only to help with migration", "3.0.0")
  private[alpakka] def cache(key: Any)(default: => Credentials) =
    _cache.getOrElse(key, {
      val credentials = default
      _cache += (key -> credentials)
      credentials
    })

}

@InternalApi
private[alpakka] trait Credentials {

  def projectId: String

  def getToken()(implicit ec: ExecutionContext, settings: GoogleSettings): Future[OAuth2BearerToken]

  def asGoogle(implicit ec: ExecutionContext, settings: GoogleSettings): GoogleCredentials
}
