/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.s3.impl

import java.net.{URLDecoder, URLEncoder}
import java.nio.charset.StandardCharsets

import akka.annotation.InternalApi
import akka.http.scaladsl.marshallers.xml.ScalaXmlSupport._
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.Uri.{Authority, Query}
import akka.http.scaladsl.model.headers.{`Raw-Request-URI`, Host, RawHeader}
import akka.http.scaladsl.model.{RequestEntity, _}
import akka.stream.alpakka.s3.AccessStyle.{PathAccessStyle, VirtualHostAccessStyle}
import akka.stream.alpakka.s3.{ApiVersion, MultipartUpload, S3Settings}
import akka.stream.scaladsl.Source
import akka.util.ByteString
import software.amazon.awssdk.regions.Region

import scala.collection.immutable.Seq
import scala.concurrent.{ExecutionContext, Future}

/**
 * Internal Api
 */
@InternalApi private[impl] object HttpRequests {

  private final val BucketPattern = "{bucket}"

  def listBuckets(headers: Seq[HttpHeader] = Nil)(implicit conf: S3Settings): HttpRequest = {
    val awsHost = Authority(Uri.Host(s"s3.amazonaws.com"))
    val (authority, scheme) = conf.endpointUrl match {
      case Some(endpointUrl) =>
        val customUri = Uri(endpointUrl)
        (customUri.authority, customUri.scheme)
      case None =>
        (awsHost, "https")
    }

    val uri = Uri(authority = authority, scheme = scheme)

    HttpRequest(HttpMethods.GET)
      .withHeaders(Host(awsHost) +: headers)
      .withUri(uri)
  }

  def listBucket(
      bucket: String,
      prefix: Option[String] = None,
      continuationToken: Option[String] = None,
      delimiter: Option[String] = None,
      headers: Seq[HttpHeader] = Nil
  )(implicit conf: S3Settings): HttpRequest = {

    val (listType, continuationTokenName) = conf.listBucketApiVersion match {
      case ApiVersion.ListBucketVersion1 => (None, "marker")
      case ApiVersion.ListBucketVersion2 => (Some("2"), "continuation-token")
    }

    val query = Query(
      Seq(
        "list-type" -> listType,
        "prefix" -> prefix,
        "delimiter" -> delimiter,
        continuationTokenName -> continuationToken
      ).collect { case (k, Some(v)) => k -> v }.toMap
    )

    HttpRequest(HttpMethods.GET)
      .withHeaders(Host(requestAuthority(bucket, conf.s3RegionProvider.getRegion)) +: headers)
      .withUri(requestUri(bucket, None).withQuery(query))
  }

  def listMultipartUploads(
      bucket: String,
      prefix: Option[String] = None,
      continuationToken: Option[ListMultipartUploadContinuationToken] = None,
      delimiter: Option[String] = None,
      headers: Seq[HttpHeader] = Nil
  )(implicit conf: S3Settings): HttpRequest = {

    val baseQuery = Seq(
      "prefix" -> prefix,
      "delimiter" -> delimiter,
      "key-marker" -> continuationToken.flatMap(_.nextKeyMarker),
      "upload-id-marker" -> continuationToken.flatMap(_.nextUploadIdMarker)
    ).collect { case (k, Some(v)) => k -> v }.toMap

    // We need to manually construct a query here because the Uri for getting a list of multipart uploads requires a
    // query param `uploads` which has no value and the current Query dsl doesn't support mixing query params that have
    // values with query params that do not have values
    val query =
      if (baseQuery.isEmpty)
        Query("uploads")
      else {
        val rest = baseQuery.map { case (k, v) => s"$k=$v" }.mkString("&")
        Query(s"uploads&$rest")
      }

    HttpRequest(HttpMethods.GET)
      .withHeaders(Host(requestAuthority(bucket, conf.s3RegionProvider.getRegion)) +: headers)
      .withUri(requestUri(bucket, None).withQuery(query))
  }

  def listParts(
      bucket: String,
      key: String,
      uploadId: String,
      continuationToken: Option[Int],
      headers: Seq[HttpHeader] = Nil
  )(implicit conf: S3Settings): HttpRequest = {

    val query = Query(
      Seq(
        "part-number-marker" -> continuationToken.map(_.toString),
        "uploadId" -> Some(uploadId)
      ).collect { case (k, Some(v)) => k -> v }.toMap
    )

    HttpRequest(HttpMethods.GET)
      .withHeaders(Host(requestAuthority(bucket, conf.s3RegionProvider.getRegion)) +: headers)
      .withUri(requestUri(bucket, Some(key)).withQuery(query))
  }

  def listObjectVersions(
      bucket: String,
      delimiter: Option[String],
      prefix: Option[String],
      continuationToken: Option[ListObjectVersionContinuationToken],
      headers: Seq[HttpHeader] = Nil
  )(implicit conf: S3Settings): HttpRequest = {

    val baseQuery = Seq(
      "bucket" -> prefix,
      "delimiter" -> delimiter,
      "prefix" -> prefix,
      "key-marker" -> continuationToken.flatMap(_.nextKeyMarker),
      "version-id-marker" -> continuationToken.flatMap(_.nextVersionIdMarker)
    ).collect { case (k, Some(v)) => k -> v }.toMap

    // We need to manually construct a query here because the Uri for getting a list of multipart uploads requires a
    // query param `uploads` which has no value and the current Query dsl doesn't support mixing query params that have
    // values with query params that do not have values
    val query =
      if (baseQuery.isEmpty)
        Query("versions")
      else {
        val rest = baseQuery.map { case (k, v) => s"$k=$v" }.mkString("&")
        Query(s"versions&$rest")
      }

    HttpRequest(HttpMethods.GET)
      .withHeaders(Host(requestAuthority(bucket, conf.s3RegionProvider.getRegion)) +: headers)
      .withUri(requestUri(bucket, None).withQuery(query))
  }

  def getDownloadRequest(s3Location: S3Location,
                         method: HttpMethod = HttpMethods.GET,
                         s3Headers: Seq[HttpHeader] = Seq.empty,
                         versionId: Option[String] = None
  )(implicit conf: S3Settings): HttpRequest = {
    val query = versionId
      .map(vId => Query("versionId" -> URLDecoder.decode(vId, StandardCharsets.UTF_8.toString)))
      .getOrElse(Query())
    s3Request(s3Location, method, _.withQuery(query))
      .withDefaultHeaders(s3Headers)
  }

  def bucketManagementRequest(
      s3Location: S3Location,
      method: HttpMethod,
      headers: Seq[HttpHeader] = Seq.empty[HttpHeader]
  )(implicit conf: S3Settings): HttpRequest =
    s3Request(s3Location = s3Location, method = method)
      .withDefaultHeaders(headers)

  def uploadManagementRequest(
      s3Location: S3Location,
      uploadId: String,
      method: HttpMethod,
      headers: Seq[HttpHeader] = Seq.empty[HttpHeader]
  )(implicit conf: S3Settings): HttpRequest =
    s3Request(s3Location,
              method,
              _.withQuery(
                Query(
                  Map(
                    "uploadId" -> uploadId
                  )
                )
              )
    )
      .withDefaultHeaders(headers)

  def uploadRequest(s3Location: S3Location,
                    payload: Source[ByteString, _],
                    contentLength: Long,
                    contentType: ContentType,
                    s3Headers: Seq[HttpHeader]
  )(implicit
      conf: S3Settings
  ): HttpRequest =
    s3Request(
      s3Location,
      HttpMethods.PUT
    ).withDefaultHeaders(s3Headers)
      .withEntity(HttpEntity(contentType, contentLength, payload))

  def initiateMultipartUploadRequest(s3Location: S3Location, contentType: ContentType, s3Headers: Seq[HttpHeader])(
      implicit conf: S3Settings
  ): HttpRequest =
    s3Request(s3Location, HttpMethods.POST, _.withQuery(Query("uploads")))
      .withDefaultHeaders(s3Headers)
      .withEntity(HttpEntity.empty(contentType))

  def uploadPartRequest(upload: MultipartUpload,
                        partNumber: Int,
                        payload: Chunk,
                        s3Headers: Seq[HttpHeader] = Seq.empty
  )(implicit conf: S3Settings): HttpRequest =
    s3Request(
      S3Location(upload.bucket, upload.key),
      HttpMethods.PUT,
      _.withQuery(Query("partNumber" -> partNumber.toString, "uploadId" -> upload.uploadId))
    ).withDefaultHeaders(s3Headers)
      .withEntity(payload.asEntity())

  def completeMultipartUploadRequest(upload: MultipartUpload, parts: Seq[(Int, String)], headers: Seq[HttpHeader])(
      implicit
      ec: ExecutionContext,
      conf: S3Settings
  ): Future[HttpRequest] = {

    //Do not let the start PartNumber,ETag and the end PartNumber,ETag be on different lines
    //  They tend to get split when this file is formatted by IntelliJ unless http://stackoverflow.com/a/19492318/1216965
    // @formatter:off
    val payload = <CompleteMultipartUpload>
                    {
                      parts.map { case (partNumber, eTag) => <Part><PartNumber>{ partNumber }</PartNumber><ETag>{ eTag }</ETag></Part> }
                    }
                  </CompleteMultipartUpload>
    // @formatter:on
    for {
      entity <- Marshal(payload).to[RequestEntity]
    } yield {
      s3Request(
        S3Location(upload.bucket, upload.key),
        HttpMethods.POST,
        _.withQuery(Query("uploadId" -> upload.uploadId))
      ).withEntity(entity).withDefaultHeaders(headers)
    }
  }

  def createBucketRegionPayload(region: Region)(implicit ec: ExecutionContext): Future[RequestEntity] = {
    //Do not let the start LocationConstraint be on different lines
    //  They tend to get split when this file is formatted by IntelliJ unless http://stackoverflow.com/a/19492318/1216965
    // @formatter:off
    val payload = <CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
      <LocationConstraint>{region.id()}</LocationConstraint>
    </CreateBucketConfiguration>
    // @formatter:on
    Marshal(payload).to[RequestEntity]
  }

  def uploadCopyPartRequest(multipartCopy: MultipartCopy,
                            sourceVersionId: Option[String] = None,
                            s3Headers: Seq[HttpHeader] = Seq.empty
  )(implicit conf: S3Settings): HttpRequest = {
    val upload = multipartCopy.multipartUpload
    val copyPartition = multipartCopy.copyPartition
    val range = copyPartition.range
    val source = copyPartition.sourceLocation.validate(conf)
    val encodedKey = URLEncoder.encode(source.key, StandardCharsets.UTF_8.toString)
    val sourceHeaderValuePrefix = s"/${source.bucket}/$encodedKey"
    val sourceHeaderValue = sourceVersionId
      .map(versionId => s"$sourceHeaderValuePrefix?versionId=$versionId")
      .getOrElse(sourceHeaderValuePrefix)
    val sourceHeader = RawHeader("x-amz-copy-source", sourceHeaderValue)
    val copyHeaders = range
      .map(br => Seq(sourceHeader, RawHeader("x-amz-copy-source-range", s"bytes=${br.first}-${br.last - 1}")))
      .getOrElse(Seq(sourceHeader))

    val allHeaders = s3Headers ++ copyHeaders

    s3Request(S3Location(upload.bucket, upload.key),
              HttpMethods.PUT,
              _.withQuery(Query("partNumber" -> copyPartition.partNumber.toString, "uploadId" -> upload.uploadId))
    )
      .withDefaultHeaders(allHeaders)
  }

  private[this] def s3Request(s3Location: S3Location, method: HttpMethod, uriFn: Uri => Uri = identity)(implicit
      conf: S3Settings
  ): HttpRequest = {
    val loc = s3Location.validate(conf)
    val s3RequestUri = uriFn(requestUri(loc.bucket, Some(loc.key)))

    HttpRequest(method)
      .withHeaders(
        Host(requestAuthority(s3Location.bucket, conf.s3RegionProvider.getRegion)),
        `Raw-Request-URI`(rawRequestUri(s3RequestUri))
      )
      .withUri(s3RequestUri)
  }

  @throws(classOf[IllegalUriException])
  private[this] def requestAuthority(bucket: String, region: Region)(implicit conf: S3Settings): Authority = {
    (conf.endpointUrl, conf.accessStyle) match {
      case (None, PathAccessStyle) =>
        Authority(Uri.Host(s"s3.$region.amazonaws.com"))

      case (None, VirtualHostAccessStyle) =>
        Authority(Uri.Host(s"$bucket.s3.$region.amazonaws.com"))

      case (Some(endpointUrl), PathAccessStyle) =>
        Uri(endpointUrl).authority

      case (Some(endpointUrl), VirtualHostAccessStyle) =>
        Uri(endpointUrl.replace(BucketPattern, bucket)).authority
    }
  }

  private[this] def requestUri(bucket: String, key: Option[String])(implicit conf: S3Settings): Uri = {
    val basePath = conf.accessStyle match {
      case PathAccessStyle =>
        Uri.Path / bucket
      case VirtualHostAccessStyle =>
        Uri.Path.Empty
    }
    val path = key.fold(basePath) { someKey =>
      someKey.split("/", -1).foldLeft(basePath)((acc, p) => acc / p)
    }
    val uri = Uri(path = path, authority = requestAuthority(bucket, conf.s3RegionProvider.getRegion))

    conf.endpointUrl match {
      case None =>
        uri.withScheme("https")
      case Some(endpointUri) =>
        uri.withScheme(Uri(endpointUri.replace(BucketPattern, "b")).scheme)
    }
  }

  private def rawRequestUri(uri: Uri): String = {
    val rawUri = uri.toHttpRequestTargetOriginForm.toString
    val rawPath = uri.path.toString()

    if (rawPath.contains("+")) {
      val fixedPath = rawPath.replaceAll("\\+", "%2B")
      require(rawUri startsWith rawPath)
      fixedPath + rawUri.drop(rawPath.length)
    } else {
      rawUri
    }
  }
}
