/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.googlecloud.bigquery.storage

import akka.annotation.InternalApi
import com.google.cloud.bigquery.storage.v1.ReadSession.TableReadOptions
import com.google.cloud.bigquery.storage.v1.stream.ReadSession
import scalapb.UnknownFieldSet

import scala.jdk.CollectionConverters._

/**
 * Internal API
 */
@InternalApi private[storage] object ProtobufConverters {

  implicit class TableReadOptionsAsScala(val readOption: TableReadOptions) {
    def asScala(): ReadSession.TableReadOptions = {
      ReadSession.TableReadOptions(
        selectedFields = selectedFields(),
        rowRestriction = readOption.getRowRestriction,
        unknownFields = unknownFields()
      )
    }

    private final def selectedFields(): Seq[String] = {
      readOption.getSelectedFieldsList.asScala.map(s => s.asInstanceOf[String]).toSeq
    }

    private final def unknownFields(): scalapb.UnknownFieldSet = {
      val map = readOption.getUnknownFields
        .asMap()
        .asScala
        .map(entry => (entry._1.asInstanceOf[Int], unknownField(entry._2)))
        .toMap
      scalapb.UnknownFieldSet(map)
    }

    private final def unknownField(field: com.google.protobuf.UnknownFieldSet.Field): UnknownFieldSet.Field = {
      UnknownFieldSet.Field(
        varint = field.getVarintList.asScala.map(_.asInstanceOf[Long]).toSeq,
        fixed64 = field.getFixed64List.asScala.map(_.asInstanceOf[Long]).toSeq,
        fixed32 = field.getFixed32List.asScala.map(_.asInstanceOf[Int]).toSeq,
        lengthDelimited = field.getLengthDelimitedList.asScala.toSeq
      )
    }

  }

}
