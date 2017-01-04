/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.s3.impl

import akka.http.scaladsl.marshallers.xml.ScalaXmlSupport._
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model.headers.{ Host, RawHeader }
import akka.http.scaladsl.model.{ RequestEntity, _ }
import akka.stream.alpakka.s3.S3Settings
import akka.stream.alpakka.s3.acl.CannedAcl
import akka.stream.scaladsl.Source
import akka.util.ByteString

import scala.collection.immutable
import scala.collection.immutable.Seq
import scala.concurrent.{ ExecutionContext, Future }

case class MetaHeaders(headers: Map[String, String])

private[alpakka] object HttpRequests {

  def getDownloadRequest(s3Location: S3Location)(implicit conf: S3Settings): HttpRequest =
    s3Request(s3Location)

  def initiateMultipartUploadRequest(s3Location: S3Location,
                                     contentType: ContentType,
                                     cannedAcl: CannedAcl,
                                     metaHeaders: MetaHeaders)(implicit conf: S3Settings): HttpRequest = {

    def buildHeaders(metaHeaders: MetaHeaders, cannedAcl: CannedAcl): immutable.Seq[HttpHeader] = {
      val metaHttpHeaders = metaHeaders.headers.map { header =>
        RawHeader(s"x-amz-meta-${header._1}", header._2)
      }(collection.breakOut): Seq[HttpHeader]
      metaHttpHeaders :+ RawHeader("x-amz-acl", cannedAcl.value)
    }

    s3Request(s3Location, HttpMethods.POST, _.withQuery(Query("uploads")))
      .withDefaultHeaders(buildHeaders(metaHeaders, cannedAcl))
      .withEntity(HttpEntity.empty(contentType))
  }

  def uploadPartRequest(upload: MultipartUpload, partNumber: Int, payload: Source[ByteString, _], payloadSize: Int)(
      implicit conf: S3Settings): HttpRequest =
    s3Request(
      upload.s3Location,
      HttpMethods.PUT,
      _.withQuery(Query("partNumber" -> partNumber.toString, "uploadId" -> upload.uploadId))
    ).withEntity(HttpEntity(ContentTypes.`application/octet-stream`, payloadSize, payload))

  def completeMultipartUploadRequest(upload: MultipartUpload, parts: Seq[(Int, String)])(
      implicit ec: ExecutionContext,
      conf: S3Settings): Future[HttpRequest] = {

    //Do not let the start PartNumber,ETag and the end PartNumber,ETag be on different lines
    //  They tend to get split when this file is formatted by IntelliJ
    val payload = <CompleteMultipartUpload>
      {parts.map { case (partNumber, etag) => <Part>
        <PartNumber>{partNumber}</PartNumber>
        <ETag>{etag}</ETag>
      </Part>
      }}
    </CompleteMultipartUpload>
    for {
      entity <- Marshal(payload).to[RequestEntity]
    } yield {
      s3Request(
        upload.s3Location,
        HttpMethods.POST,
        _.withQuery(Query("uploadId" -> upload.uploadId))
      ).withEntity(entity)
    }
  }

  private[this] def s3Request(s3Location: S3Location,
                              method: HttpMethod = HttpMethods.GET,
                              uriFn: (Uri => Uri) = identity)(implicit conf: S3Settings): HttpRequest = {

    def requestHost(s3Location: S3Location)(implicit conf: S3Settings): Uri.Host =
      conf.proxy match {
        case None => Uri.Host(s"${s3Location.bucket}.s3.amazonaws.com")
        case Some(proxy) => Uri.Host(proxy.host)
      }

    def requestUri(s3Location: S3Location)(implicit conf: S3Settings): Uri = {
      val uri = Uri(s"/${s3Location.key}").withHost(requestHost(s3Location)).withScheme("https")
      conf.proxy match {
        case None => uri
        case Some(proxy) => uri.withPort(proxy.port)
      }
    }

    HttpRequest(method).withHeaders(Host(requestHost(s3Location))).withUri(uriFn(requestUri(s3Location)))
  }
}
