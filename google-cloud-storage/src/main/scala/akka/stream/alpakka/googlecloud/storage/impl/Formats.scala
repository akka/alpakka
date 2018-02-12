/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.storage.impl

import akka.stream.alpakka.googlecloud.storage.Model.{BucketInfo, BucketListResult, StorageObject}
import play.api.libs.json.{JsPath, Json, Reads}

trait Formats {

  import play.api.libs.functional.syntax._

  implicit val storageObjectFormat = Json.format[StorageObject]
  implicit val bucketInfoFormat = Json.format[BucketInfo]

  implicit val bucketListResultReads: Reads[BucketListResult] = (
    (JsPath \ "kind").read[String] and
    (JsPath \ "nextPageToken").readNullable[String] and
    (JsPath \ "prefixes").readNullable[List[String]] and
    (JsPath \ "items").readWithDefault[List[StorageObject]](List.empty)
  )(BucketListResult.apply _)

}
