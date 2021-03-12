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

import scala.concurrent.{Await, ExecutionContext, Future}

@InternalApi
private[alpakka] object Credentials {

  def apply(c: Config)(implicit system: ClassicActorSystemProvider): Credentials = c.getString("provider") match {
    case "service-account" => ServiceAccountCredentials(c.getConfig("service-account"))
    case "compute-engine" =>
      val timeout = c.getDuration("compute-engine.timeout").asScala
      Await.result(ComputeEngineCredentials(), timeout)
  }

}

@InternalApi
private[alpakka] trait Credentials {

  def projectId: String

  def getToken()(implicit ec: ExecutionContext, settings: GoogleSettings): Future[OAuth2BearerToken]

  def asGoogle(implicit ec: ExecutionContext, settings: GoogleSettings): GoogleCredentials
}
