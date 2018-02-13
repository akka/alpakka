/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.storage.impl

import akka.stream.alpakka.googlecloud.storage.Model.{BucketInfo, BucketListResult, StorageObject}
import spray.json.{DefaultJsonProtocol, JsValue, RootJsonReader}

@akka.annotation.InternalApi
object Formats extends DefaultJsonProtocol {

  private final case class BucketListResultJson(kind: String,
                                                nextPageToken: Option[String],
                                                prefixes: Option[List[String]],
                                                items: Option[List[StorageObject]])

  implicit val storageObjectFormat = jsonFormat8(StorageObject)
  implicit val bucketInfoFormat = jsonFormat4(BucketInfo)

  private implicit val bucketListResultJsonReads = jsonFormat4(BucketListResultJson)

  implicit object BucketListResultReads extends RootJsonReader[BucketListResult] {
    override def read(json: JsValue): BucketListResult = {
      val res = bucketListResultJsonReads.read(json)
      BucketListResult(
        res.kind,
        res.nextPageToken,
        res.prefixes,
        res.items.getOrElse(List.empty)
      )
    }
  }

}
