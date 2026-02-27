/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.google.auth

import akka.actor.ClassicActorSystemProvider
import akka.annotation.InternalApi
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.{ContentTypes, FormData, HttpEntity, HttpMethods, HttpRequest, Uri}
import akka.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken, RawHeader}
import akka.stream.Materializer
import akka.stream.alpakka.google.http.GoogleHttp
import akka.stream.alpakka.google.{implicits, RequestSettings}
import akka.util.ByteString
import org.slf4j.LoggerFactory

import java.time.{Clock, Instant}
import scala.concurrent.{ExecutionContext, Future}
import spray.json._

import java.nio.file.{Files, Path}
import scala.concurrent.duration.DurationInt

@InternalApi
private[alpakka] object ExternalAccountCredentials {

  def apply(json: JsValue, scopes: Seq[String])(implicit system: ClassicActorSystemProvider): Credentials = {
    require(scopes.nonEmpty && scopes.forall(_.nonEmpty),
            "External account credentials requires that at least one scope is specified.")

    val credentials = json.convertTo[ExternalAccount]
    new ExternalAccountCredentials(scopes, credentials)
  }

  private case class ExternalAccount(
      `type`: String,
      audience: String,
      subject_token_type: String,
      token_url: String,
      credential_source: CredentialSource,
      service_account_impersonation_url: Option[String]
  )

  private object ExternalAccount {
    import spray.json.DefaultJsonProtocol._
    implicit val format: RootJsonFormat[ExternalAccount] = jsonFormat6(ExternalAccount.apply)
  }

  private case class CredentialSource(
      // Credentials source is a file
      file: Option[String],
      // Credential source is a URL
      url: Option[String],
      // A third credential source is certificate, not implemented yet

      // Headers for URL
      headers: Option[Map[String, String]],
      // If not present, the default format is the text of the token
      format: Option[CredentialSourceFormat]
  )

  private object CredentialSource {
    import spray.json.DefaultJsonProtocol._
    implicit val format: RootJsonFormat[CredentialSource] = jsonFormat4(CredentialSource.apply)
  }

  private case class CredentialSourceFormat(`type`: String, subject_token_field_name: Option[String])

  private object CredentialSourceFormat {
    import spray.json.DefaultJsonProtocol._
    implicit val format: RootJsonFormat[CredentialSourceFormat] = jsonFormat2(CredentialSourceFormat.apply)
  }

  private case class ImpersonationRequest(delegates: Seq[String], scope: Seq[String], lifetime: String)

  private object ImpersonationRequest {
    import spray.json.DefaultJsonProtocol._
    implicit val format: RootJsonFormat[ImpersonationRequest] = jsonFormat3(ImpersonationRequest.apply)
  }

  private case class ImpersonationResponse(accessToken: String, expireTime: Instant)

  private object ImpersonationResponse {
    import spray.json.DefaultJsonProtocol._

    implicit object InstantJsonFormat extends JsonFormat[Instant] {
      override def write(obj: Instant): JsValue =
        JsString(obj.toString)

      override def read(json: JsValue): Instant = json match {
        case JsString(s) =>
          try Instant.parse(s) // Parses the ISO 8601 string back to an Instant
          catch {
            case e: Exception =>
              deserializationError(s"Expected Instant as a valid ISO 8601 string, but got '$s'", e)
          }
        case other =>
          deserializationError(s"Expected Instant as a JsString, but got ${other.getClass.getSimpleName}")
      }
    }

    implicit val format: RootJsonFormat[ImpersonationResponse] = jsonFormat2(ImpersonationResponse.apply)
  }
}

import ExternalAccountCredentials._

/**
 * Getting an access token for a workload identity account requires the following three steps:
 *
 *   1. Obtain a token from the credentials source. The credential source, including the headers and tokens to authenticate with it, is described in the JSON.
 *   2. Use that token to obtain an STS token from the STS token URL.
 *   3. Use that token to impersonate a service account, the URL for doing that is supplied in the JSON.
 */
@InternalApi
private[alpakka] class ExternalAccountCredentials private (scopes: Seq[String], externalAccount: ExternalAccount)(
    implicit system: ClassicActorSystemProvider
) extends OAuth2Credentials(None) {

  private val log = LoggerFactory.getLogger(this.getClass)

  import GoogleOAuth2Exception._
  import SprayJsonSupport._
  import implicits._

  private val TokenExchangeGrantType = "urn:ietf:params:oauth:grant-type:token-exchange"
  private val AccessTokenTokenType = "urn:ietf:params:oauth:token-type:access_token"
  private val DefaultTokenLifetime = 3600.seconds

  override protected def getAccessToken()(implicit mat: Materializer,
                                          settings: RequestSettings,
                                          clock: Clock): Future[AccessToken] = {
    import mat.executionContext
    for {
      subjectToken <- obtainSubjectToken()
      workloadIdentityAccessToken <- obtainStsToken(subjectToken)
      // If requested to impersonate a service account, then impersonate it
      accessToken <- externalAccount.service_account_impersonation_url match {
        case Some(url) => impersonateServiceAccount(url, workloadIdentityAccessToken.token)
        case None => Future.successful(workloadIdentityAccessToken)
      }
    } yield accessToken
  }

  private def obtainSubjectToken()(implicit ec: ExecutionContext, settings: RequestSettings): Future[String] = {
    log.debug("Obtaining subject token from credential source")
    externalAccount.credential_source.url
      .map(obtainSubjectTokenFromUrl)
      .orElse(externalAccount.credential_source.file.map(obtainSubjectTokenFromFile))
      .getOrElse(Future.failed(new RuntimeException("Neither a file nor url credential source was specified")))
      .map { bytes =>
        val format = externalAccount.credential_source.format.getOrElse(CredentialSourceFormat("text", None))
        format.`type` match {
          case "text" =>
            bytes.utf8String
          case "json" =>
            format.subject_token_field_name match {
              case Some(fieldName) =>
                val jsonBody = JsonParser(bytes.toArrayUnsafe())
                jsonBody.asJsObject.fields.get(fieldName).getOrElse {
                  throw new RuntimeException(
                    s"JSON returned from credential source does not contain field $fieldName, fields were: ${jsonBody.asJsObject.fields.keys
                      .mkString(", ")}"
                  )
                } match {
                  case JsString(value) =>
                    log.debug("Subject token obtained")
                    value
                  case other =>
                    throw new RuntimeException(
                      s"Token at field $fieldName returned from credential source is not a string, but instead is a ${other.getClass.getSimpleName}"
                    )
                }
              case None =>
                throw new RuntimeException("No subject_token_field_name in credential source format")
            }
        }
      }
  }

  private def obtainSubjectTokenFromUrl(url: String)(implicit settings: RequestSettings): Future[ByteString] = {
    val request = HttpRequest(
      method = HttpMethods.GET,
      uri = Uri(url),
      headers = externalAccount.credential_source.headers
        .getOrElse(Map.empty)
        .map {
          case (key, value) => RawHeader(key, value)
        }
        .toSeq
    )
    GoogleHttp().singleRequest[ByteString](request)
  }

  private def obtainSubjectTokenFromFile(file: String): Future[ByteString] = {
    Future.successful(ByteString.fromArrayUnsafe(Files.readAllBytes(Path.of(file))))
  }

  private def obtainStsToken(
      subjectToken: String
  )(implicit ec: ExecutionContext, settings: RequestSettings, clock: Clock): Future[AccessToken] = {
    log.debug("Obtaining STS token from {}", externalAccount.token_url)
    // API docs for the exchange: https://datatracker.ietf.org/doc/html/rfc8693
    // Google specific info is here about what each parameter supports, note though that Googles documented request
    // body is JSON with camel case parameters. We ignore that, and stick to the OAuth2 spec, which is what the Google
    // SDK libraries do:
    // https://docs.cloud.google.com/iam/docs/reference/sts/rest/v1/TopLevel/token
    val formData = Seq(
        "grant_type" -> TokenExchangeGrantType,
        "subject_token_type" -> externalAccount.subject_token_type,
        "subject_token" -> subjectToken,
        "requested_token_type" -> AccessTokenTokenType,
        "audience" -> externalAccount.audience
      ) ++ scopes.map("scope" -> _)
    val request = HttpRequest(
      method = HttpMethods.POST,
      uri = Uri(externalAccount.token_url),
      entity = FormData(formData: _*).toEntity
    )

    GoogleHttp().singleRequest[AccessToken](request).map { token =>
      log.debug("STS token obtained")
      token
    }
  }

  private def impersonateServiceAccount(
      serviceAccountImpersonationUrl: String,
      accessToken: String
  )(implicit ec: ExecutionContext, settings: RequestSettings): Future[AccessToken] = {
    log.debug("Impersonating service account at URL {}", serviceAccountImpersonationUrl)
    // API docs for the exchange: https://docs.cloud.google.com/iam/docs/reference/credentials/rest/v1/projects.serviceAccounts/generateAccessToken
    val requestBody = CompactPrinter(ImpersonationRequest(Nil, scopes, s"${DefaultTokenLifetime.toSeconds}s").toJson)
    val request = HttpRequest(
      method = HttpMethods.POST,
      uri = Uri(serviceAccountImpersonationUrl),
      entity = HttpEntity(ContentTypes.`application/json`, requestBody),
      headers = Seq(Authorization(OAuth2BearerToken(accessToken)))
    )
    GoogleHttp().singleRequest[ImpersonationResponse](request).map { response =>
      log.debug("Obtained access token for service account impersonation")
      AccessToken(response.accessToken, response.expireTime.getEpochSecond)
    }
  }
}
