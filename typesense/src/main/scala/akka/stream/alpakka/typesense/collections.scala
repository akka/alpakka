/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.typesense

import java.time.Instant

final case class CollectionSchema(name: String,
                                  fields: Seq[Field],
                                  tokenSeparators: Option[Seq[String]] = None,
                                  symbolsToIndex: Option[Seq[String]] = None,
                                  defaultSortingField: Option[String] = None)

final case class Field(name: String,
                       `type`: FieldType,
                       optional: Option[Boolean] = None,
                       facet: Option[Boolean] = None,
                       index: Option[Boolean] = None)

final case class CollectionResponse(name: String,
                                    numDocuments: Int,
                                    fields: Seq[FieldResponse],
                                    defaultSortingField: String,
                                    createdAt: Instant)

//TODO: shouldn't flags be optional for compatibility with older Typesense versions?
//TODO: add infix, sort, locale
final case class FieldResponse(name: String, `type`: FieldType, optional: Boolean, facet: Boolean, index: Boolean)

sealed trait FieldType

object FieldType {
  case object String extends FieldType
  case object StringArray extends FieldType
  case object Int32 extends FieldType
  case object Int32Array extends FieldType
  case object Int64 extends FieldType
  case object Int64Array extends FieldType
  case object Float extends FieldType
  case object FloatArray extends FieldType
  case object Bool extends FieldType
  case object BoolArray extends FieldType
  case object Geopoint extends FieldType
  case object GeopointArray extends FieldType
  case object StringAutoArray extends FieldType
  case object Auto extends FieldType
}
