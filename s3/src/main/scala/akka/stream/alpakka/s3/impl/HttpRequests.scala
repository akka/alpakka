/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.s3.impl

import akka.http.scaladsl.marshallers.xml.ScalaXmlSupport._
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model.headers.{Host, RawHeader}
import akka.http.scaladsl.model.{RequestEntity, _}
import akka.stream.alpakka.s3.S3Settings
import akka.stream.alpakka.s3.acl.CannedAcl
import akka.stream.scaladsl.Source
import akka.util.ByteString
import scala.collection.immutable
import scala.collection.immutable.Seq
import scala.concurrent.{ExecutionContext, Future}

case class MetaHeaders(headers: Map[String, String])

private[alpakka] object HttpRequests {

  def getDownloadRequest(s3Location: S3Location, region: String)(implicit conf: S3Settings): HttpRequest =
    s3Request(s3Location, region: String)


  def listBuckets(
    s3PrefixLocation: S3PrefixLocation,
    region: String,
    maxKeys: Option[Int],
    marker: Option[String]
  )(implicit conf: S3Settings): HttpRequest = {
    val prefixQuery: Map[String, String] = s3PrefixLocation.prefix match {
      case Some(p) => Map("prefix" -> p)
      case None => Map[String, String]().empty
    }

    val maxKeysQuery: Map[String, String] = maxKeys match {
      case Some(m) => Map("max-keys" -> m.toString)
      case _ => Map[String, String]().empty
    }

    val markerQuery: Map[String, String] = marker match {
      case Some(m) => Map("marker" -> m)
      case _ => Map[String, String]().empty
    }

    HttpRequest(HttpMethods.GET)
      .withHeaders(Host(requestHost(region)))
      .withUri(
        requestUri(s3PrefixLocation.bucket, region).withQuery(Uri.Query(prefixQuery ++ markerQuery ++ maxKeysQuery))
      )
  }

  def initiateMultipartUploadRequest(s3Location: S3Location,
                                     contentType: ContentType,
                                     cannedAcl: CannedAcl,
                                     region: String,
                                     metaHeaders: MetaHeaders)(implicit conf: S3Settings): HttpRequest = {

    def buildHeaders(metaHeaders: MetaHeaders, cannedAcl: CannedAcl): immutable.Seq[HttpHeader] = {
      val metaHttpHeaders = metaHeaders.headers.map { header =>
        RawHeader(s"x-amz-meta-${header._1}", header._2)
      }(collection.breakOut): Seq[HttpHeader]
      metaHttpHeaders :+ RawHeader("x-amz-acl", cannedAcl.value)
    }

    s3Request(s3Location, region, HttpMethods.POST, _.withQuery(Query("uploads")))
      .withDefaultHeaders(buildHeaders(metaHeaders, cannedAcl))
      .withEntity(HttpEntity.empty(contentType))
  }

  def uploadPartRequest(upload: MultipartUpload,
                        partNumber: Int,
                        payload: Source[ByteString, _],
                        payloadSize: Int,
                        region: String)(implicit conf: S3Settings): HttpRequest =
    s3Request(
      upload.s3Location,
      region,
      HttpMethods.PUT,
      _.withQuery(Query("partNumber" -> partNumber.toString, "uploadId" -> upload.uploadId))
    ).withEntity(HttpEntity(ContentTypes.`application/octet-stream`, payloadSize, payload))

  def completeMultipartUploadRequest(upload: MultipartUpload, parts: Seq[(Int, String)], region: String)(
      implicit ec: ExecutionContext,
      conf: S3Settings): Future[HttpRequest] = {

    //Do not let the start PartNumber,ETag and the end PartNumber,ETag be on different lines
    //  They tend to get split when this file is formatted by IntelliJ unless http://stackoverflow.com/a/19492318/1216965
    // @formatter:off
    val payload = <CompleteMultipartUpload>
                    {
                      parts.map { case (partNumber, etag) => <Part><PartNumber>{ partNumber }</PartNumber><ETag>{ etag }</ETag></Part> }
                    }
                  </CompleteMultipartUpload>
    // @formatter:on
    for {
      entity <- Marshal(payload).to[RequestEntity]
    } yield {
      s3Request(
        upload.s3Location,
        region,
        HttpMethods.POST,
        _.withQuery(Query("uploadId" -> upload.uploadId))
      ).withEntity(entity)
    }
  }

  private[this] def s3Request(s3Location: S3Location,
                              region: String,
                              method: HttpMethod = HttpMethods.GET,
                              uriFn: (Uri => Uri) = identity)(implicit conf: S3Settings): HttpRequest = {

    HttpRequest(method)
      .withHeaders(Host(requestHost(region)))
      .withUri(uriFn(requestUriWithKey(s3Location.bucket, s3Location.key, region)))
  }

  private[this] def requestHost(region: String)(implicit conf: S3Settings): Uri.Host =
    conf.proxy match {
      case None =>
        region match {
          case "us-east-1" => Uri.Host("s3.amazonaws.com")
          case _ => Uri.Host(s"s3-$region.amazonaws.com")
        }
      case Some(proxy) => Uri.Host(proxy.host)
    }

  private[this] def requestUri(bucket: String, region: String)(implicit conf: S3Settings): Uri = {
    val uri = Uri(s"/$bucket").withHost(requestHost(region))
    conf.proxy match {
      case None => uri.withScheme("https")
      case Some(proxy) => uri.withPort(proxy.port).withScheme(proxy.schema)
    }
  }

  private[this] def requestUriWithKey(bucket: String, key: String, region: String)(implicit conf: S3Settings): Uri = {
    val uri = Uri(s"/$bucket/$key").withHost(requestHost(region))
    conf.proxy match {
      case None => uri.withScheme("https")
      case Some(proxy) => uri.withPort(proxy.port).withScheme(proxy.schema)
    }
  }
}
