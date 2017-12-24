/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.s3.impl

import akka.http.scaladsl.marshallers.xml.ScalaXmlSupport._
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.Uri.{Authority, Query}
import akka.http.scaladsl.model.headers.Host
import akka.http.scaladsl.model.{ContentTypes, RequestEntity, _}
import akka.stream.alpakka.s3.S3Settings
import akka.stream.scaladsl.Source
import akka.util.ByteString

import scala.collection.immutable.Seq
import scala.concurrent.{ExecutionContext, Future}

private[alpakka] object HttpRequests {

  def listBucket(
      bucket: String,
      prefix: Option[String] = None,
      continuationToken: Option[String] = None
  )(implicit conf: S3Settings): HttpRequest = {

    val query = Query(
      Seq(
        "list-type" -> Some("2"),
        "prefix" -> prefix,
        "continuation-token" -> continuationToken
      ).collect { case (k, Some(v)) => k -> v }.toMap
    )

    HttpRequest(HttpMethods.GET)
      .withHeaders(Host(requestHost(bucket, conf.s3RegionProvider.getRegion)))
      .withUri(requestUri(bucket, None).withQuery(query))
  }

  def getDownloadRequest(s3Location: S3Location,
                         method: HttpMethod = HttpMethods.GET,
                         s3Headers: S3Headers = S3Headers.empty)(implicit conf: S3Settings): HttpRequest =
    s3Request(s3Location, method)
      .withDefaultHeaders(s3Headers.headers: _*)

  def uploadRequest(s3Location: S3Location,
                    payload: Source[ByteString, _],
                    contentLength: Long,
                    contentType: ContentType,
                    s3Headers: S3Headers)(
      implicit conf: S3Settings
  ): HttpRequest =
    s3Request(
      s3Location,
      HttpMethods.PUT
    ).withDefaultHeaders(s3Headers.headers: _*)
      .withEntity(HttpEntity(contentType, contentLength, payload))

  def initiateMultipartUploadRequest(s3Location: S3Location, contentType: ContentType, s3Headers: S3Headers)(
      implicit conf: S3Settings
  ): HttpRequest =
    s3Request(s3Location, HttpMethods.POST, _.withQuery(Query("uploads")))
      .withDefaultHeaders(s3Headers.headers: _*)
      .withEntity(HttpEntity.empty(contentType))

  def uploadPartRequest(upload: MultipartUpload,
                        partNumber: Int,
                        payload: Source[ByteString, _],
                        payloadSize: Int,
                        s3Headers: S3Headers = S3Headers.empty)(implicit conf: S3Settings): HttpRequest =
    s3Request(
      upload.s3Location,
      HttpMethods.PUT,
      _.withQuery(Query("partNumber" -> partNumber.toString, "uploadId" -> upload.uploadId))
    ).withDefaultHeaders(s3Headers.headers: _*)
      .withEntity(HttpEntity(ContentTypes.`application/octet-stream`, payloadSize, payload))

  def completeMultipartUploadRequest(upload: MultipartUpload, parts: Seq[(Int, String)])(
      implicit ec: ExecutionContext,
      conf: S3Settings
  ): Future[HttpRequest] = {

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
        HttpMethods.POST,
        _.withQuery(Query("uploadId" -> upload.uploadId))
      ).withEntity(entity)
    }
  }

  private[this] def s3Request(s3Location: S3Location,
                              method: HttpMethod = HttpMethods.GET,
                              uriFn: (Uri => Uri) = identity)(implicit conf: S3Settings): HttpRequest =
    HttpRequest(method)
      .withHeaders(Host(requestHost(s3Location.bucket, conf.s3RegionProvider.getRegion)))
      .withUri(uriFn(requestUri(s3Location.bucket, Some(s3Location.key))))

  @throws(classOf[IllegalUriException])
  private[this] def requestHost(bucket: String, region: String)(implicit conf: S3Settings): Uri.Host =
    conf.proxy match {
      case None =>
        if (!conf.pathStyleAccess) {
          val bucketRegex = "[^a-z0-9\\-\\.]{1,255}|[\\.]{2,}".r
          bucketRegex.findFirstIn(bucket) match {
            case Some(illegalCharacter) =>
              throw IllegalUriException(
                "Bucket name contains non-LDH characters",
                s"""The following character is not allowed: $illegalCharacter
                   | This may be solved by setting akka.stream.alpakka.s3.path-style-access to true in the configuration.
                 """.stripMargin
              )
            case None => ()
          }
        }
        region match {
          case "us-east-1" =>
            if (conf.pathStyleAccess) {
              Uri.Host("s3.amazonaws.com")
            } else {
              Uri.Host(s"$bucket.s3.amazonaws.com")
            }
          case _ =>
            if (conf.pathStyleAccess) {
              Uri.Host(s"s3-$region.amazonaws.com")
            } else {
              Uri.Host(s"$bucket.s3-$region.amazonaws.com")
            }
        }
      case Some(proxy) => Uri.Host(proxy.host)
    }

  private[this] def requestUri(bucket: String, key: Option[String])(implicit conf: S3Settings): Uri = {
    val basePath = if (conf.pathStyleAccess) {
      Uri.Path / bucket
    } else {
      Uri.Path.Empty
    }
    val path = key.fold(basePath) { someKey =>
      someKey.split("/").foldLeft(basePath)((acc, p) => acc / p)
    }
    val uri = Uri(path = path, authority = Authority(requestHost(bucket, conf.s3RegionProvider.getRegion)))
    conf.proxy match {
      case None => uri.withScheme("https").withHost(requestHost(bucket, conf.s3RegionProvider.getRegion))
      case Some(proxy) => uri.withPort(proxy.port).withScheme(proxy.scheme).withHost(proxy.host)
    }
  }
}
