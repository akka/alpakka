/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.dynamodb.impl

import java.io.{ByteArrayInputStream, InputStream}
import java.util.concurrent.atomic.AtomicInteger

import akka.NotUsed
import akka.actor.ActorSystem
import akka.annotation.InternalApi
import akka.http.scaladsl.Http.HostConnectionPool
import akka.http.scaladsl.model.{ContentType, HttpEntity, _}
import akka.stream._
import akka.stream.alpakka.dynamodb.impl.AwsClient.{AwsConnect, AwsRequestMetadata}
import akka.stream.alpakka.dynamodb.{AwsClientSettings, AwsOp}
import akka.stream.scaladsl.{Flow, Source}
import com.amazonaws.auth.AWS4Signer
import com.amazonaws.http.{HttpMethodName, HttpResponseHandler, HttpResponse => AWSHttpResponse}
import com.amazonaws.services.dynamodbv2.model.{
  InternalServerErrorException,
  ItemCollectionSizeLimitExceededException,
  LimitExceededException,
  ProvisionedThroughputExceededException,
  RequestLimitExceededException
}
import com.amazonaws.{DefaultRequest, ResponseMetadata, HttpMethod => _, _}

import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions
import scala.util.{Failure, Success, Try}

/**
 * INTERNAL API
 */
@InternalApi
private[dynamodb] object AwsClient {

  type AwsConnect =
    Flow[(HttpRequest, AwsRequestMetadata), (Try[HttpResponse], AwsRequestMetadata), HostConnectionPool]

  case class AwsRequestMetadata(id: Long, op: AwsOp)

}

/**
 * INTERNAL API
 */
@InternalApi
private[dynamodb] trait AwsClient[S <: AwsClientSettings] {

  protected implicit def system: ActorSystem

  protected implicit def materializer: Materializer

  protected implicit def ec: ExecutionContext

  private lazy val signer = {
    val s = new AWS4Signer()
    s.setServiceName(service)
    s.setRegionName(settings.region)
    s
  }
  protected val settings: S
  protected val connection: AwsConnect
  protected val service: String
  protected val defaultContentType: ContentType
  protected val errorResponseHandler: HttpResponseHandler[AmazonServiceException]

  private val requestId = new AtomicInteger()
  private val credentials = settings.credentialsProvider
  private val signableUrl = Uri(url)
  private val uri = new java.net.URI(url)
  private val decider: Supervision.Decider = _ => Supervision.Stop

  private implicit def method(method: HttpMethodName): HttpMethod = method match {
    case HttpMethodName.POST => HttpMethods.POST
    case HttpMethodName.GET => HttpMethods.GET
    case HttpMethodName.PUT => HttpMethods.PUT
    case HttpMethodName.DELETE => HttpMethods.DELETE
    case HttpMethodName.HEAD => HttpMethods.HEAD
    case HttpMethodName.OPTIONS => HttpMethods.OPTIONS
    case HttpMethodName.PATCH => HttpMethods.PATCH
  }

  def flow[Op <: AwsOp](op: Op): Flow[Op, Op#B, NotUsed] = {
    val opFlow =
      Flow[Op]
        .map(op => toAwsRequest(op))
        .via(connection)
        .mapAsync(settings.parallelism) {
          case (Success(response), i) => toAwsResult(response, i)
          case (Failure(ex), _) => Future.failed(ex)
        }

    opFlow
      .via(
        new RecoverWithRetry(
          settings.retrySettings.maximumRetries,
          settings.retrySettings.initialRetryTimeout,
          settings.retrySettings.backoffStrategy, {
            case _ @(_: InternalServerErrorException | _: ItemCollectionSizeLimitExceededException |
                _: LimitExceededException | _: ProvisionedThroughputExceededException |
                _: RequestLimitExceededException) =>
              Source.single(op).via(opFlow)
          }
        )
      )
      .withAttributes(ActorAttributes.supervisionStrategy(decider))
      .map(_.asInstanceOf[Op#B])
  }

  private def toAwsRequest(s: AwsOp): (HttpRequest, AwsRequestMetadata) = {
    val original = s.marshaller.marshall(s.request)
    original.setEndpoint(uri)
    original.getHeaders.remove("Content-Type")
    original.getHeaders.remove("Content-Length")
    signer.sign(original, credentials.getCredentials)

    val amzHeaders = original.getHeaders
    val body = read(original.getContent)

    val tokenHeader: List[headers.RawHeader] = {
      credentials.getCredentials match {
        case _: auth.AWSSessionCredentials =>
          Some(headers.RawHeader("x-amz-security-token", amzHeaders.get("X-Amz-Security-Token")))
        case _ =>
          None
      }
    }.toList

    val httpr = HttpRequest(
      uri = signableUrl,
      method = original.getHttpMethod,
      headers = List(
        headers.RawHeader("x-amz-date", amzHeaders.get("X-Amz-Date")),
        headers.RawHeader("authorization", amzHeaders.get("Authorization")),
        headers.RawHeader("x-amz-target", amzHeaders.get("X-Amz-Target"))
      ) ++ tokenHeader,
      entity = HttpEntity(defaultContentType, body)
    )

    httpr -> AwsRequestMetadata(requestId.getAndIncrement(), s)
  }

  private def read(in: InputStream) = Stream.continually(in.read).takeWhile(-1 != _).map(_.toByte).toArray

  private def toAwsResult(
      response: HttpResponse,
      metadata: AwsRequestMetadata
  ): Future[AmazonWebServiceResult[ResponseMetadata]] = {
    val req = new DefaultRequest(this.service)
    val awsResp = new AWSHttpResponse(req, null) //
    response.entity.dataBytes.runFold(Array.emptyByteArray)(_ ++ _).flatMap { bytes =>
      awsResp.setContent(new ByteArrayInputStream(bytes))
      awsResp.setStatusCode(response.status.intValue)
      awsResp.setStatusText(response.status.defaultMessage)
      if (200 <= awsResp.getStatusCode && awsResp.getStatusCode < 300) {
        val handle = metadata.op.handler.handle(awsResp)
        val resp = handle.getResult
        Future.successful(resp)
      } else {
        response.headers.foreach { h =>
          awsResp.addHeader(h.name, h.value)
        }
        Future.failed(errorResponseHandler.handle(awsResp))
      }
    }
  }

  protected def url: String = s"https://${settings.host}/"

}
