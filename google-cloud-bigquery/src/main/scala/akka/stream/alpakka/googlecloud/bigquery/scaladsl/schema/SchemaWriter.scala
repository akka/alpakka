/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.scaladsl.schema

import akka.stream.alpakka.googlecloud.bigquery.model.TableJsonProtocol.{
  NullableMode,
  RepeatedMode,
  RequiredMode,
  TableFieldSchema
}

trait SchemaWriter[-T] {

  def write(name: String, mode: FieldMode): TableFieldSchema

}

sealed abstract class FieldMode
case object Nullable extends FieldMode {
  override def toString: String = NullableMode
}
case object Required extends FieldMode {
  override def toString: String = RequiredMode
}
case object Repeated extends FieldMode {
  override def toString: String = RepeatedMode
}
