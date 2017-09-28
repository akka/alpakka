/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.oracle.bmcs.impl

import akka.http.scaladsl.model._
import akka.http.scaladsl.model.Uri.{Authority, Query}
import akka.http.scaladsl.model.headers.Host
import akka.stream.alpakka.oracle.bmcs.BmcsSettings
import akka.stream.scaladsl.Source
import akka.util.ByteString

import scala.collection.immutable.Seq

object HttpRequests {

  def listObjectsRequest(bucket: String, start: Option[String] = None, prefix: Option[String] = None)(
      implicit conf: BmcsSettings
  ): HttpRequest = {
    val query = Query(
      Seq(
        "start" -> start,
        "prefix" -> prefix
      ).collect { case (k, Some(v)) => k -> v }.toMap
    )
    HttpRequest(HttpMethods.GET)
      .withHeaders(Host(requestHost))
      .withUri(requestUri(bucket, Some("o")).withQuery(query))
  }

  def getDownloadRequest(bucket: String, objectName: String)(implicit conf: BmcsSettings): HttpRequest =
    HttpRequest(HttpMethods.GET)
      .withHeaders(Host(requestHost))
      .withUri(requestUri(bucket, Some(s"o/$objectName")))

  def completeMultipartUploadRequest(bucket: String, objectName: String, uploadId: String)(
      implicit conf: BmcsSettings
  ): HttpRequest =
    HttpRequest(HttpMethods.POST)
      .withHeaders(Host(requestHost))
      .withUri(requestUri(bucket, Some(s"u/$objectName")).withQuery(Query("uploadId" -> uploadId)))

  def initiateMultipartUploadRequest(bucket: String)(implicit conf: BmcsSettings): HttpRequest =
    HttpRequest(HttpMethods.POST)
      .withHeaders(Host(requestHost))
      .withUri(requestUri(bucket, Some("u")))

  def uploadPartRequest(bucket: String,
                        objectName: String,
                        partNumber: Int,
                        uploadId: String,
                        payload: Source[ByteString, _],
                        payloadSize: Int)(implicit conf: BmcsSettings): HttpRequest = {
    val query = Query(
      Seq(
        "uploadId" -> uploadId,
        "uploadPartNum" -> partNumber.toString
      ).toMap
    )

    val request = HttpRequest(HttpMethods.PUT)
      .withHeaders(Host(requestHost))
      .withUri(requestUri(bucket, Some(s"u/$objectName")).withQuery(query))
      .withEntity(HttpEntity(ContentTypes.`application/octet-stream`, payloadSize, payload))
    request
  }

  private[this] def requestUri(bucket: String, key: Option[String] = None)(implicit conf: BmcsSettings): Uri = {
    val basePath = Uri.Path / "n" / conf.namespace / "b" / bucket
    val path = key.fold(basePath) { someKey =>
      someKey.split("/").foldLeft(basePath)((acc, p) => acc / p)
    }
    val uri = Uri(path = path, authority = Authority(requestHost))
    conf.proxy match {
      case None => uri.withScheme("https").withHost(requestHost)
      case Some(proxy) => uri.withPort(proxy.port).withScheme(proxy.scheme).withHost(proxy.host)
    }
  }

  private[this] def requestHost(implicit conf: BmcsSettings): Uri.Host =
    conf.proxy match {
      case None =>
        Uri.Host(s"objectstorage.${conf.region}.oraclecloud.com")
      case Some(proxy) => Uri.Host(proxy.host)
    }

}
