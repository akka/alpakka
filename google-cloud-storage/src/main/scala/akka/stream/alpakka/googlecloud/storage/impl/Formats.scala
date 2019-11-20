/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.storage.impl

import akka.http.scaladsl.model.{ContentType, ContentTypes}
import akka.stream.alpakka.googlecloud.storage._
import spray.json.{DefaultJsonProtocol, JsValue, RootJsonReader}

import java.time.OffsetDateTime

import scala.util.Try

@akka.annotation.InternalApi
object Formats extends DefaultJsonProtocol {

  /**
   * Google API list bucket response
   *
   * https://cloud.google.com/storage/docs/json_api/v1/objects/list
   */
  private final case class BucketListResultJson(
      kind: String,
      nextPageToken: Option[String],
      prefixes: Option[List[String]],
      items: Option[List[StorageObjectJson]]
  )

  /**
   * Google API storage response object
   *
   * https://cloud.google.com/storage/docs/json_api/v1/objects#resource
   */
  private final case class StorageObjectJson(
      kind: String,
      id: String,
      name: String,
      bucket: String,
      generation: String,
      contentType: Option[String],
      size: String,
      etag: String,
      md5Hash: String,
      crc32c: String,
      mediaLink: String,
      selfLink: String,
      updated: String,
      timeCreated: String,
      storageClass: String,
      contentEncoding: String,
      contentLanguage: String
  )

  /**
   * Google API rewrite response object
   *
   * https://cloud.google.com/storage/docs/json_api/v1/objects/rewrite
   */
  private final case class RewriteResponseJson(
      kind: String,
      totalBytesRewritten: String,
      objectSize: String,
      done: Boolean,
      rewriteToken: Option[String],
      resource: Option[StorageObjectJson]
  )

  /**
   * Google API bucket response object
   *
   * https://cloud.google.com/storage/docs/json_api/v1/buckets#resource
   */
  private final case class BucketInfoJson(
      name: String,
      location: String,
      kind: String,
      id: String,
      selfLink: String,
      etag: String
  )

  private implicit val bucketInfoJsonFormat = jsonFormat6(BucketInfoJson)
  private implicit val storageObjectJsonFormat = jsonFormat17(StorageObjectJson)
  private implicit val bucketListResultJsonReads = jsonFormat4(BucketListResultJson)
  private implicit val rewriteReponseFormat = jsonFormat6(RewriteResponseJson)

  implicit val bucketInfoFormat = jsonFormat2(BucketInfo)

  implicit object BucketListResultReads extends RootJsonReader[BucketListResult] {
    override def read(json: JsValue): BucketListResult = {
      val res = bucketListResultJsonReads.read(json)
      BucketListResult(
        res.kind,
        res.nextPageToken,
        res.prefixes,
        res.items.getOrElse(List.empty).map(storageObjectJsonToStorageObject)
      )
    }
  }

  implicit object StorageObjectReads extends RootJsonReader[StorageObject] {
    override def read(
        json: JsValue
    ): StorageObject = {
      val res = storageObjectJsonFormat.read(json)
      storageObjectJsonToStorageObject(res)
    }
  }

  implicit object RewriteResponseReads extends RootJsonReader[RewriteResponse] {
    override def read(json: JsValue): RewriteResponse = {
      val res = rewriteReponseFormat.read(json)

      val totalBytesRewritten =
        Try(res.totalBytesRewritten.toLong)
          .getOrElse(throw new RuntimeException("Rewrite response totalBytesRewritten is not of Long type"))

      val objectSize =
        Try(res.objectSize.toLong)
          .getOrElse(throw new RuntimeException("Rewrite response objectSize is not of Long type"))

      RewriteResponse(
        res.kind,
        totalBytesRewritten,
        objectSize,
        res.done,
        res.rewriteToken,
        res.resource.map(storageObjectJsonToStorageObject)
      )
    }
  }

  implicit object BucketReads extends RootJsonReader[Bucket] {
    override def read(
        json: JsValue
    ): Bucket = {
      val res = bucketInfoJsonFormat.read(json)

      Bucket(
        res.name,
        res.location,
        res.kind,
        res.id,
        res.selfLink,
        res.etag
      )
    }
  }

  private def storageObjectJsonToStorageObject(storageObjectJson: StorageObjectJson): StorageObject = {
    def strToLongOrThrow(str: String, fieldName: String) =
      Try(str.toLong)
        .getOrElse(throw new RuntimeException(s"Storage object $fieldName is not of type Long"))

    def strToDateTimeOrThrow(str: String, fieldName: String) =
      Try(OffsetDateTime.parse(str))
        .getOrElse(throw new RuntimeException(s"Storage object $fieldName is not a valid OffsetDateTime"))

    StorageObject(
      storageObjectJson.kind,
      storageObjectJson.id,
      storageObjectJson.name,
      storageObjectJson.bucket,
      storageObjectJson.generation.toLong,
      storageObjectJson.contentType.map(parseContentType).getOrElse(ContentTypes.`application/octet-stream`),
      strToLongOrThrow(storageObjectJson.size, "size"),
      storageObjectJson.etag,
      storageObjectJson.md5Hash,
      storageObjectJson.crc32c,
      storageObjectJson.mediaLink,
      storageObjectJson.selfLink,
      strToDateTimeOrThrow(storageObjectJson.updated, "updated"),
      strToDateTimeOrThrow(storageObjectJson.timeCreated, "timeCreated"),
      storageObjectJson.storageClass,
      storageObjectJson.contentEncoding,
      storageObjectJson.contentLanguage
    )
  }

  private def parseContentType(contentType: String): ContentType =
    ContentType.parse(contentType) match {
      case Left(_) => throw new RuntimeException(s"Storage object content type $contentType is not supported")
      case Right(ct) => ct
    }
}
