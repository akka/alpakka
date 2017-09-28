/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.oracle.bmcs.impl

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

import scala.collection.immutable.Seq

//https://docs.us-phoenix-1.oraclecloud.com/api/#/en/objectstorage/20160918/ObjectSummary/
final case class ObjectSummary(name: String, size: Option[Long], md5: Option[String], timeCreated: Option[String])

//https://docs.us-phoenix-1.oraclecloud.com/api/#/en/objectstorage/20160918/ListObjects/
final case class ListObjects(objects: Seq[ObjectSummary], prefixes: Option[Seq[String]], nextStartWith: Option[String])

//https://docs.us-phoenix-1.oraclecloud.com/api/#/en/objectstorage/20160918/requests/CreateMultipartUploadDetails
final case class CreateMultipartUploadDetails(`object`: String,
                                              contentType: Option[String] = None,
                                              contentLanguage: Option[String] = None,
                                              contentEncoding: Option[String] = None,
                                              metadata: Option[String] = None)

//https://docs.us-phoenix-1.oraclecloud.com/api/#/en/objectstorage/20160918/MultipartUpload/
final case class MultipartUpload(namespace: String,
                                 bucket: String,
                                 `object`: String,
                                 uploadId: String,
                                 timeCreated: String) // time  as described in RFC 2616, section 14.29

//https://docs.us-phoenix-1.oraclecloud.com/api/#/en/objectstorage/20160918/requests/CommitMultipartUploadPartDetails
final case class CommitMultipartUploadPartDetails(partNum: Int, etag: String)

//https://docs.us-phoenix-1.oraclecloud.com/api/#/en/objectstorage/20160918/requests/CommitMultipartUploadPartDetails
final case class CommitMultipartUploadDetails(partsToCommit: Seq[CommitMultipartUploadPartDetails],
                                              partsToExclude: Option[Seq[Int]])

/**
 * provides implicit json formats for spray json support.
 */
trait BmcsJsonSupport extends SprayJsonSupport with DefaultJsonProtocol {

  implicit val objectsSummaryFormat: RootJsonFormat[ObjectSummary] = jsonFormat4(ObjectSummary)
  implicit val listObjectsFormat: RootJsonFormat[ListObjects] = jsonFormat3(ListObjects)
  implicit val createMultipartUploadDetailsFormat: RootJsonFormat[CreateMultipartUploadDetails] = jsonFormat5(
    CreateMultipartUploadDetails
  )
  implicit val ultipartUploadFormat: RootJsonFormat[MultipartUpload] = jsonFormat5(MultipartUpload)
  implicit val commitMultipartUploadPartDetailsFormat: RootJsonFormat[CommitMultipartUploadPartDetails] = jsonFormat2(
    CommitMultipartUploadPartDetails
  )
  implicit val commitMultipartUploadDetailsFormat: RootJsonFormat[CommitMultipartUploadDetails] = jsonFormat2(
    CommitMultipartUploadDetails
  )

}
