/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.typesense

import akka.annotation.InternalApi

import java.time.Instant
import scala.compat.java8.OptionConverters.RichOptionForJava8
import scala.jdk.CollectionConverters.{CollectionHasAsScala, SeqHasAsJava}
import scala.jdk.OptionConverters.RichOptional

final class CollectionSchema @InternalApi private[typesense] (val name: String,
                                                              val fields: Seq[Field],
                                                              val tokenSeparators: Option[Seq[String]],
                                                              val symbolsToIndex: Option[Seq[String]],
                                                              val defaultSortingField: Option[String]) {
  def getFields(): java.util.List[Field] = fields.asJava

  def getTokenSeparators(): java.util.Optional[java.util.List[String]] = tokenSeparators.asJava.map(_.asJava)

  def getSymbolsToIndex(): java.util.Optional[java.util.List[String]] = symbolsToIndex.asJava.map(_.asJava)

  def getDefaultSortingField(): java.util.Optional[String] = defaultSortingField.asJava

  def withTokenSeparators(tokenSeparators: Seq[String]): CollectionSchema =
    new CollectionSchema(name, fields, Some(tokenSeparators), symbolsToIndex, defaultSortingField)

  def withTokenSeparators(tokenSeparators: java.util.List[String]): CollectionSchema =
    new CollectionSchema(name, fields, Some(tokenSeparators.asScala.toSeq), symbolsToIndex, defaultSortingField)

  def withSymbolsToIndex(symbolsToIndex: Seq[String]): CollectionSchema =
    new CollectionSchema(name, fields, tokenSeparators, Some(symbolsToIndex), defaultSortingField)

  def withSymbolsToIndex(symbolsToIndex: java.util.List[String]): CollectionSchema =
    new CollectionSchema(name, fields, tokenSeparators, Some(symbolsToIndex.asScala.toSeq), defaultSortingField)

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

  def create(name: String, fields: java.util.List[Field]): CollectionSchema =
    new CollectionSchema(name, fields.asScala.toSeq, None, None, None)

  def apply(name: String, fields: Seq[Field], tokenSeparators: Option[Seq[String]]): CollectionSchema =
    new CollectionSchema(name, fields, tokenSeparators, None, None)

  def create(name: String,
             fields: java.util.List[Field],
             tokenSeparators: java.util.Optional[java.util.List[String]]): CollectionSchema =
    new CollectionSchema(name, fields.asScala.toSeq, tokenSeparators.toScala.map(_.asScala.toSeq), None, None)

  def apply(name: String,
            fields: Seq[Field],
            tokenSeparators: Option[Seq[String]],
            symbolsToIndex: Option[Seq[String]]): CollectionSchema =
    new CollectionSchema(name, fields, tokenSeparators, symbolsToIndex, None)

  def create(name: String,
             fields: java.util.List[Field],
             tokenSeparators: java.util.Optional[java.util.List[String]],
             symbolsToIndex: java.util.Optional[java.util.List[String]]): CollectionSchema =
    new CollectionSchema(name,
                         fields.asScala.toSeq,
                         tokenSeparators.toScala.map(_.asScala.toSeq),
                         symbolsToIndex.toScala.map(_.asScala.toSeq),
                         None)

  def apply(name: String,
            fields: Seq[Field],
            tokenSeparators: Option[Seq[String]],
            symbolsToIndex: Option[Seq[String]],
            defaultSortingField: Option[String]): CollectionSchema =
    new CollectionSchema(name, fields, tokenSeparators, symbolsToIndex, defaultSortingField)

  def create(name: String,
             fields: java.util.List[Field],
             tokenSeparators: java.util.Optional[java.util.List[String]],
             symbolsToIndex: java.util.Optional[java.util.List[String]],
             defaultSortingField: java.util.Optional[String]): CollectionSchema =
    new CollectionSchema(name,
                         fields.asScala.toSeq,
                         tokenSeparators.toScala.map(_.asScala.toSeq),
                         symbolsToIndex.toScala.map(_.asScala.toSeq),
                         defaultSortingField.toScala)
}

final class Field @InternalApi private[typesense] (val name: String,
                                                   val `type`: FieldType,
                                                   val optional: Option[Boolean],
                                                   val facet: Option[Boolean],
                                                   val index: Option[Boolean]) {
  def getOptional(): java.util.Optional[Boolean] = optional.asJava

  def getFacet(): java.util.Optional[Boolean] = facet.asJava

  def getIndes(): java.util.Optional[Boolean] = index.asJava

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

  def create(name: String, `type`: FieldType): Field = new Field(name, `type`, None, None, None)

  def apply(name: String, `type`: FieldType, optional: Option[Boolean]): Field =
    new Field(name, `type`, optional, None, None)

  def create(name: String, `type`: FieldType, optional: java.util.Optional[Boolean]): Field =
    new Field(name, `type`, optional.toScala, None, None)

  def apply(name: String, `type`: FieldType, optional: Option[Boolean], facet: Option[Boolean]): Field =
    new Field(name, `type`, optional, facet, None)

  def create(name: String,
             `type`: FieldType,
             optional: java.util.Optional[Boolean],
             facet: java.util.Optional[Boolean]): Field =
    new Field(name, `type`, optional.toScala, facet.toScala, None)

  def apply(name: String,
            `type`: FieldType,
            optional: Option[Boolean],
            facet: Option[Boolean],
            index: Option[Boolean]): Field = new Field(name, `type`, optional, facet, index)

  def create(name: String,
             `type`: FieldType,
             optional: java.util.Optional[Boolean],
             facet: java.util.Optional[Boolean],
             index: java.util.Optional[Boolean]): Field =
    new Field(name, `type`, optional.toScala, facet.toScala, index.toScala)
}

final class CollectionResponse @InternalApi private[typesense] (val name: String,
                                                                val numDocuments: Int,
                                                                val fields: Seq[FieldResponse],
                                                                val defaultSortingField: String,
                                                                val createdAt: Instant) {
  def getNumDocuments(): java.lang.Integer = java.lang.Integer.valueOf(numDocuments)

  def getFields(): java.util.List[FieldResponse] = fields.asJava

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

  def create(name: String,
             numDocuments: java.lang.Integer,
             fields: java.util.List[FieldResponse],
             defaultSortingField: String,
             createdAt: Instant): CollectionResponse =
    new CollectionResponse(name, scala.Int.box(numDocuments), fields.asScala.toSeq, defaultSortingField, createdAt)
}

//TODO: add infix, sort, locale
final class FieldResponse @InternalApi private[typesense] (val name: String,
                                                           val `type`: FieldType,
                                                           val facet: Boolean,
                                                           val optional: Option[Boolean],
                                                           val index: Option[Boolean]) {

  def getOptional(): java.util.Optional[Boolean] = optional.asJava

  def getIndex(): java.util.Optional[Boolean] = index.asJava

  def withOptional(optional: Boolean): FieldResponse = new FieldResponse(name, `type`, facet, Some(optional), index)

  def withIndex(index: Boolean): FieldResponse = new FieldResponse(name, `type`, facet, optional, Some(index))

  override def equals(other: Any): Boolean = other match {
    case that: FieldResponse =>
      name == that.name &&
      `type` == that.`type` &&
      facet == that.facet &&
      optional == that.optional &&
      index == that.index
    case _ => false
  }

  override def hashCode(): Int = java.util.Objects.hash(name, `type`, facet, optional, index)

  override def toString = s"FieldResponse(name=$name, `type`=${`type`}, facet=$facet, optional=$optional, index=$index)"
}

object FieldResponse {
  def apply(name: String, `type`: FieldType, facet: Boolean): FieldResponse =
    new FieldResponse(name, `type`, facet, None, None)

  def apply(name: String, `type`: FieldType, facet: Boolean, optional: Option[Boolean]): FieldResponse =
    new FieldResponse(name, `type`, facet, optional, None)

  def apply(name: String,
            `type`: FieldType,
            facet: Boolean,
            optional: Option[Boolean],
            index: Option[Boolean]): FieldResponse =
    new FieldResponse(name, `type`, facet, optional, index)

  def create(name: String, `type`: FieldType, facet: Boolean): FieldResponse =
    new FieldResponse(name, `type`, facet, None, None)

  def create(name: String, `type`: FieldType, facet: Boolean, optional: java.util.Optional[Boolean]): FieldResponse =
    new FieldResponse(name, `type`, facet, optional.toScala, None)

  def create(name: String,
             `type`: FieldType,
             facet: Boolean,
             optional: java.util.Optional[Boolean],
             index: java.util.Optional[Boolean]): FieldResponse =
    new FieldResponse(name, `type`, facet, optional.toScala, index.toScala)
}

sealed abstract class FieldType

object FieldType {
  sealed abstract class String extends FieldType
  case object String extends String

  /**
   * Java API
   */
  def string: String = String

  sealed abstract class StringArray extends FieldType
  case object StringArray extends StringArray

  /**
   * Java API
   */
  def stringArray: StringArray = StringArray

  sealed abstract class Int32 extends FieldType
  case object Int32 extends Int32

  /**
   * Java API
   */
  def int32: Int32 = Int32

  sealed abstract class Int32Array extends FieldType
  case object Int32Array extends Int32Array

  /**
   * Java API
   */
  def int32Array: Int32Array = Int32Array

  sealed abstract class Int64 extends FieldType
  case object Int64 extends Int64

  /**
   * Java API
   */
  def int64: Int64 = Int64

  sealed abstract class Int64Array extends FieldType
  case object Int64Array extends Int64Array

  /**
   * Java API
   */
  def int64Array: Int64Array = Int64Array

  sealed abstract class Float extends FieldType
  case object Float extends Float

  /**
   * Java API
   */
  def float: Float = Float

  sealed abstract class FloatArray extends FieldType
  case object FloatArray extends FloatArray

  /**
   * Java API
   */
  def floatArray: FloatArray = FloatArray

  sealed abstract class Bool extends FieldType
  case object Bool extends Bool

  /**
   * Java API
   */
  def bool: Bool = Bool

  sealed abstract class BoolArray extends FieldType
  case object BoolArray extends BoolArray

  /**
   * Java API
   */
  def boolArray: BoolArray = BoolArray

  sealed abstract class Geopoint extends FieldType
  case object Geopoint extends Geopoint

  /**
   * Java API
   */
  def geopoint: Geopoint = Geopoint

  sealed abstract class GeopointArray extends FieldType
  case object GeopointArray extends GeopointArray

  /**
   * Java API
   */
  def geopointArray: GeopointArray = GeopointArray

  sealed abstract class StringAutoArray extends FieldType
  case object StringAutoArray extends StringAutoArray

  /**
   * Java API
   */
  def stringAutoArray: StringAutoArray = StringAutoArray

  sealed abstract class Auto extends FieldType
  case object Auto extends Auto

  /**
   * Java API
   */
  def auto: Auto = Auto
}
