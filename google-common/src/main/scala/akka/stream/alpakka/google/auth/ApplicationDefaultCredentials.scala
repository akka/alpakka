/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.google.auth

import akka.actor.ClassicActorSystemProvider
import com.typesafe.config.Config
import spray.json.{JsString, JsonParser}

import scala.io.Source
import scala.jdk.CollectionConverters._

object ApplicationDefaultCredentials {

  /** Java API */
  def create(c: Config, scopes: java.lang.Iterable[String], system: ClassicActorSystemProvider): Credentials =
    apply(c, scopes.asScala.toSeq)(system)

  /** Scala API */
  def apply(c: Config, scopes: Seq[String])(implicit system: ClassicActorSystemProvider): Credentials =
    apply(c.getString("path"), scopes)

  /** Java API */
  def create(path: String, scopes: java.lang.Iterable[String], system: ClassicActorSystemProvider): Credentials =
    apply(path, scopes.asScala.toSeq)(system)

  /** Scala API */
  def apply(path: String, scopes: Seq[String])(implicit system: ClassicActorSystemProvider): Credentials = {
    val src = Source.fromFile(path)
    try {
      loadFromJson(src.mkString, scopes)
    } finally {
      src.close()
    }
  }

  /** Java API */
  def loadFromJson(json: String, scopes: java.lang.Iterable[String], system: ClassicActorSystemProvider): Credentials =
    loadFromJson(json, scopes.asScala.toSeq)(system)

  /** Scala API */
  def loadFromJson(json: String, scopes: Seq[String])(implicit system: ClassicActorSystemProvider): Credentials = {
    val credentials = JsonParser(json)
    val credentialsType = credentials.asJsObject().fields.get("type") match {
      case Some(JsString(value)) => value
      case Some(v) =>
        throw new RuntimeException(s"Google application credentials JSON file has type field that isn't a string: $v")
      case None => throw new RuntimeException("Google application credentials JSON file has no type field")
    }

    credentialsType match {
      // GoogleCredentials.USER_FILE_TYPE
      case "authorized_user" =>
        UserAccessCredentials(credentials)

      // GoogleCredentials.SERVICE_ACCOUNT_FILE_TYPE
      case "service_account" =>
        ServiceAccountCredentials(credentials, scopes)

      // ExternalAccountCredentials.EXTERNAL_ACCOUNT_FILE_TYPE
      case "external_account" =>
        ExternalAccountCredentials(credentials, scopes)

      case unsupported =>
        throw new RuntimeException(s"Unsupported Google application credentials JSON file type: $unsupported")
    }
  }
}
