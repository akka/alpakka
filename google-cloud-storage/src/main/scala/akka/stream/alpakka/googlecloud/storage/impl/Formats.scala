/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.storage.impl

import java.time.OffsetDateTime

import akka.http.scaladsl.model.{ContentType, ContentTypes}
import akka.stream.alpakka.googlecloud.storage._
import spray.json.{DefaultJsonProtocol, JsObject, JsValue, RootJsonFormat, RootJsonReader}

import scala.util.Try

@akka.annotation.InternalApi
object Formats extends DefaultJsonProtocol {

  private final case class CustomerEncryption(encryptionAlgorithm: String, keySha256: String)
  private implicit val customerEncryptionJsonFormat = jsonFormat2(CustomerEncryption)

  private final case class Owner(entity: String, entityId: Option[String])
  private implicit val OwnerJsonFormat = jsonFormat2(Owner)

  private final case class ProjectTeam(projectNumber: String, team: String)
  private implicit val ProjectTeamJsonFormat = jsonFormat2(ProjectTeam)

  private final case class ObjectAccessControls(kind: String,
                                                id: String,
                                                selfLink: String,
                                                bucket: String,
                                                `object`: String,
                                                generation: String,
                                                entity: String,
                                                role: String,
                                                email: String,
                                                entityId: String,
                                                domain: String,
                                                projectTeam: ProjectTeam,
                                                etag: String)
  private implicit val ObjectAccessControlsJsonFormat = jsonFormat13(ObjectAccessControls)

  /**
   * Google API storage response object
   *
   * https://cloud.google.com/storage/docs/json_api/v1/objects#resource
   */
  private final case class StorageObjectJson(readable: StorageObjectReadOnlyJson, writeable: StorageObjectWriteableJson)

  // private sub class of StorageObjectJson used to workaround 22 field jsonFormat issue
  private final case class StorageObjectReadOnlyJson(
      bucket: String,
      componentCount: Option[Int],
      customerEncryption: Option[CustomerEncryption],
      etag: String,
      generation: String,
      id: String,
      kind: String,
      kmsKeyName: Option[String],
      mediaLink: String,
      metageneration: String,
      owner: Option[Owner],
      retentionExpirationTime: Option[String],
      selfLink: String,
      size: String,
      timeCreated: String,
      timeDeleted: Option[String],
      timeStorageClassUpdated: String,
      updated: String
  )

  private implicit val storageObjectReadOnlyJson = jsonFormat18(StorageObjectReadOnlyJson)

  // private sub class of StorageObjectJson used to workaround 22 field jsonFormat issue
  private final case class StorageObjectWriteableJson(
      cacheControl: Option[String],
      contentDisposition: Option[String],
      contentEncoding: Option[String],
      contentLanguage: Option[String],
      contentType: Option[String],
      crc32c: String,
      eventBasedHold: Option[Boolean],
      md5Hash: Option[String],
      metadata: Option[Map[String, String]],
      name: String,
      storageClass: String,
      temporaryHold: Option[Boolean],
      acl: Option[List[ObjectAccessControls]]
  )

  private implicit val storageObjectWritableJson = jsonFormat13(StorageObjectWriteableJson)

  private implicit object StorageObjectJsonFormat extends RootJsonFormat[StorageObjectJson] {
    override def read(value: JsValue): StorageObjectJson = {
      val readOnlyFields = value.convertTo[StorageObjectReadOnlyJson]
      val writeableFields = value.convertTo[StorageObjectWriteableJson]
      StorageObjectJson(readOnlyFields, writeableFields)
    }
    override def write(obj: StorageObjectJson): JsValue = {
      val fields1 = obj.readable.toJson.asJsObject.fields
      val fields2 = obj.writeable.toJson.asJsObject.fields
      JsObject(fields1 ++ fields2)
    }
  }

  implicit object StorageObjectReads extends RootJsonReader[StorageObject] {
    override def read(json: JsValue): StorageObject = {
      val res = StorageObjectJsonFormat.read(json)
      storageObjectJsonToStorageObject(res)
    }
  }

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

  private implicit val bucketInfoJsonFormat = jsonFormat6(BucketInfoJson)

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

  private implicit val rewriteResponseFormat = jsonFormat6(RewriteResponseJson)

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

  private implicit val bucketListResultJsonReads = jsonFormat4(BucketListResultJson)

  implicit object RewriteResponseReads extends RootJsonReader[RewriteResponse] {
    override def read(json: JsValue): RewriteResponse = {
      val res = rewriteResponseFormat.read(json)

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

    import storageObjectJson.readable._
    import storageObjectJson.writeable._
    StorageObject(
      kind,
      id,
      name,
      bucket,
      generation.toLong,
      contentType.map(parseContentType).getOrElse(ContentTypes.`application/octet-stream`),
      strToLongOrThrow(size, "size"),
      etag,
      md5Hash,
      crc32c,
      mediaLink,
      selfLink,
      strToDateTimeOrThrow(updated, "updated"),
      strToDateTimeOrThrow(timeCreated, "timeCreated"),
      timeDeleted.map(td => strToDateTimeOrThrow(td, "timeDeleted")),
      storageClass,
      contentDisposition,
      contentEncoding,
      contentLanguage,
      strToLongOrThrow(metageneration, "metageneration"),
      temporaryHold,
      eventBasedHold,
      retentionExpirationTime.map(ret => strToDateTimeOrThrow(ret, "retentionExpirationTime")),
      strToDateTimeOrThrow(timeStorageClassUpdated, "retentionExpirationTime"),
      cacheControl,
      metadata,
      componentCount,
      kmsKeyName,
      customerEncryption.map(
        ce =>
          akka.stream.alpakka.googlecloud.storage
            .CustomerEncryption(ce.encryptionAlgorithm, ce.keySha256)
      ),
      owner.map(o => akka.stream.alpakka.googlecloud.storage.Owner(o.entity, o.entityId)),
      acl.map(
        _.map(
          a =>
            akka.stream.alpakka.googlecloud.storage.ObjectAccessControls(
              a.kind,
              a.id,
              a.selfLink,
              a.bucket,
              a.`object`,
              a.generation,
              a.entity,
              a.role,
              a.email,
              a.entityId,
              a.domain,
              akka.stream.alpakka.googlecloud.storage
                .ProjectTeam(a.projectTeam.projectNumber, a.projectTeam.team),
              a.etag
            )
        )
      )
    )
  }

  private def parseContentType(contentType: String): ContentType =
    ContentType.parse(contentType) match {
      case Left(_) => throw new RuntimeException(s"Storage object content type $contentType is not supported")
      case Right(ct) => ct
    }
}
