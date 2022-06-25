/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.typesense

import java.time.Instant

final class CollectionSchema private (val name: String,
                                      val fields: Seq[Field],
                                      val tokenSeparators: Option[Seq[String]],
                                      val symbolsToIndex: Option[Seq[String]],
                                      val defaultSortingField: Option[String]) {
  def withTokenSeparators(tokenSeparators: Seq[String]): CollectionSchema =
    new CollectionSchema(name, fields, Some(tokenSeparators), symbolsToIndex, defaultSortingField)

  def withSymbolsToIndex(symbolsToIndex: Seq[String]): CollectionSchema =
    new CollectionSchema(name, fields, tokenSeparators, Some(symbolsToIndex), defaultSortingField)

  def withDefaultSortingField(defaultSortingField: String): CollectionSchema =
    new CollectionSchema(name, fields, tokenSeparators, symbolsToIndex, Some(defaultSortingField))

  override def equals(other: Any): Boolean = other match {
    case that: CollectionSchema =>
      name == that.name &&
      fields == that.fields &&
      tokenSeparators == that.tokenSeparators &&
      symbolsToIndex == that.symbolsToIndex &&
      defaultSortingField == that.defaultSortingField
    case _ => false
  }

  override def hashCode: Int =
    java.util.Objects.hash(name, fields, tokenSeparators, symbolsToIndex, defaultSortingField)

  override def toString =
    s"CollectionSchema(name=$name, fields=$fields, tokenSeparators=$tokenSeparators, symbolsToIndex=$symbolsToIndex, defaultSortingField=$defaultSortingField)"
}

object CollectionSchema {
  def apply(name: String, fields: Seq[Field]): CollectionSchema =
    new CollectionSchema(name, fields, None, None, None)

  def apply(name: String, fields: Seq[Field], tokenSeparators: Option[Seq[String]]): CollectionSchema =
    new CollectionSchema(name, fields, tokenSeparators, None, None)

  def apply(name: String,
            fields: Seq[Field],
            tokenSeparators: Option[Seq[String]],
            symbolsToIndex: Option[Seq[String]]): CollectionSchema =
    new CollectionSchema(name, fields, tokenSeparators, symbolsToIndex, None)

  def apply(name: String,
            fields: Seq[Field],
            tokenSeparators: Option[Seq[String]],
            symbolsToIndex: Option[Seq[String]],
            defaultSortingField: Option[String]): CollectionSchema =
    new CollectionSchema(name, fields, tokenSeparators, symbolsToIndex, defaultSortingField)
}

final class Field private (val name: String,
                           val `type`: FieldType,
                           val optional: Option[Boolean],
                           val facet: Option[Boolean],
                           val index: Option[Boolean]) {
  def withOptional(optional: Boolean): Field = new Field(name, `type`, Some(optional), facet, index)
  def withFacet(facet: Boolean): Field = new Field(name, `type`, optional, Some(facet), index)
  def withIndex(index: Boolean): Field = new Field(name, `type`, optional, facet, Some(index))

  override def equals(other: Any): Boolean = other match {
    case that: Field =>
      name == that.name &&
      `type` == that.`type` &&
      optional == that.optional &&
      facet == that.facet &&
      index == that.index
    case _ => false
  }

  override def hashCode: Int = java.util.Objects.hash(name, name, `type`, optional, facet, index)

  override def toString = s"Field(name=$name, `type`=${`type`}, optional=$optional, facet=$facet, index=$index)"
}

object Field {
  def apply(name: String, `type`: FieldType): Field = new Field(name, `type`, None, None, None)

  def apply(name: String, `type`: FieldType, optional: Option[Boolean]): Field =
    new Field(name, `type`, optional, None, None)

  def apply(name: String, `type`: FieldType, optional: Option[Boolean], facet: Option[Boolean]): Field =
    new Field(name, `type`, optional, facet, None)

  def apply(name: String,
            `type`: FieldType,
            optional: Option[Boolean],
            facet: Option[Boolean],
            index: Option[Boolean]): Field = new Field(name, `type`, optional, facet, index)
}

final class CollectionResponse private (val name: String,
                                        val numDocuments: Int,
                                        val fields: Seq[FieldResponse],
                                        val defaultSortingField: String,
                                        val createdAt: Instant) {

  override def equals(other: Any): Boolean = other match {
    case that: CollectionResponse =>
      name == that.name &&
      numDocuments == that.numDocuments &&
      fields == that.fields &&
      defaultSortingField == that.defaultSortingField &&
      createdAt == that.createdAt
    case _ => false
  }

  override def hashCode: Int = java.util.Objects.hash(name, numDocuments, fields, defaultSortingField, createdAt)

  override def toString =
    s"CollectionResponse(name=$name, numDocuments=$numDocuments, fields=$fields, defaultSortingField=$defaultSortingField, createdAt=$createdAt)"
}

object CollectionResponse {
  def apply(name: String,
            numDocuments: Int,
            fields: Seq[FieldResponse],
            defaultSortingField: String,
            createdAt: Instant): CollectionResponse =
    new CollectionResponse(name, numDocuments, fields, defaultSortingField, createdAt)
}

//TODO: add infix, sort, locale
final class FieldResponse private (val name: String,
                                   val `type`: FieldType,
                                   val optional: Boolean,
                                   val facet: Boolean,
                                   val index: Boolean) {

  override def equals(other: Any): Boolean = other match {
    case that: FieldResponse =>
      name == that.name &&
      `type` == that.`type` &&
      optional == that.optional &&
      facet == that.facet &&
      index == that.index
    case _ => false
  }
  override def hashCode: Int = java.util.Objects.hash(name, `type`, optional, facet, index)

  override def toString = s"FieldResponse(name=$name, `type`=${`type`}, optional=$optional, facet=$facet, index=$index)"
}

object FieldResponse {
  def apply(name: String, `type`: FieldType, optional: Boolean, facet: Boolean, index: Boolean): FieldResponse =
    new FieldResponse(name, `type`, optional, facet, index)
}

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
