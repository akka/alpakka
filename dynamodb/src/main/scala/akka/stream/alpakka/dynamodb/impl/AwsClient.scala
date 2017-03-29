/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.dynamodb.impl

import java.io.{ByteArrayInputStream, InputStream}
import java.util.concurrent.atomic.AtomicInteger

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http.HostConnectionPool
import akka.http.scaladsl.model.{ContentType, HttpEntity, _}
import akka.stream.alpakka.dynamodb.AwsOp
import akka.stream.alpakka.dynamodb.impl.AwsClient.{AwsConnect, AwsRequestMetadata}
import akka.stream.scaladsl.Flow
import akka.stream.{ActorAttributes, ActorMaterializer, Supervision}
import com.amazonaws.auth.{AWS4Signer, DefaultAWSCredentialsProviderChain}
import com.amazonaws.http.{HttpMethodName, HttpResponseHandler, HttpResponse => AWSHttpResponse}
import com.amazonaws.{DefaultRequest, HttpMethod => _, _}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

private[alpakka] object AwsClient {

  case class AwsRequestMetadata(id: Long, op: AwsOp)

  type AwsConnect =
    Flow[(HttpRequest, AwsRequestMetadata), (Try[HttpResponse], AwsRequestMetadata), HostConnectionPool]

}

private[alpakka] trait AwsClient[S <: ClientSettings] {

  protected implicit def system: ActorSystem

  protected implicit def materializer: ActorMaterializer

  protected implicit def ec: ExecutionContext

  protected val settings: S
  protected val connection: AwsConnect
  protected val service: String
  protected val defaultContentType: ContentType
  protected val errorResponseHandler: HttpResponseHandler[AmazonServiceException]

  private val requestId = new AtomicInteger()
  private val credentials = new DefaultAWSCredentialsProviderChain()

  private lazy val signer = {
    val s = new AWS4Signer()
    s.setServiceName(service)
    s.setRegionName(settings.region)
    s
  }

  private implicit def method(method: HttpMethodName): HttpMethod = method match {
    case HttpMethodName.POST => HttpMethods.POST
    case HttpMethodName.GET => HttpMethods.GET
    case HttpMethodName.PUT => HttpMethods.PUT
    case HttpMethodName.DELETE => HttpMethods.DELETE
    case HttpMethodName.HEAD => HttpMethods.HEAD
    case HttpMethodName.OPTIONS => HttpMethods.OPTIONS
    case HttpMethodName.PATCH => HttpMethods.PATCH
  }

  private val signableUrl = Uri("https://" + settings.host + "/")

  private val decider: Supervision.Decider = { case _ => Supervision.Stop }

  def flow: Flow[AwsOp, AmazonWebServiceResult[ResponseMetadata], NotUsed] =
    Flow[AwsOp]
      .map(toAwsRequest)
      .via(connection)
      .mapAsync(settings.parallelism) {
        case (Success(response), i) => toAwsResult(response, i)
        case (Failure(ex), i) => Future.failed(ex)
      }
      .withAttributes(ActorAttributes.supervisionStrategy(decider))

  private def toAwsRequest(s: AwsOp): (HttpRequest, AwsRequestMetadata) = {
    val original = s.marshaller.marshall(s.request)
    original.setEndpoint(new java.net.URI("https://" + settings.host + "/"))
    original.getHeaders.remove("Content-Type")
    original.getHeaders.remove("Content-Length")
    signer.sign(original, credentials.getCredentials)

    val amzHeaders = original.getHeaders
    val body = read(original.getContent)

    val httpr = HttpRequest(
      uri = signableUrl,
      method = original.getHttpMethod,
      headers = List(
        headers.RawHeader("x-amz-date", amzHeaders.get("X-Amz-Date")),
        headers.RawHeader("authorization", amzHeaders.get("Authorization")),
        headers.RawHeader("x-amz-target", amzHeaders.get("X-Amz-Target"))
      ),
      entity = HttpEntity(defaultContentType, body)
    )

    httpr -> AwsRequestMetadata(requestId.getAndIncrement(), s)
  }

  private def toAwsResult(response: HttpResponse,
                          metadata: AwsRequestMetadata): Future[AmazonWebServiceResult[ResponseMetadata]] = {
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

  private def read(in: InputStream) = Stream.continually(in.read).takeWhile(-1 != _).map(_.toByte).toArray

}
