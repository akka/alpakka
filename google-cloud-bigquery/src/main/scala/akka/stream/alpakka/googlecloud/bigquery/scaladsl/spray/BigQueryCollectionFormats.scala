/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.scaladsl.spray

import spray.json._

import scala.collection.immutable
import scala.reflect.ClassTag

trait BigQueryCollectionFormats {

  /**
   * Supplies the BigQueryJsonFormat for Lists.
   */
  implicit def listFormat[T: BigQueryJsonFormat]: BigQueryJsonFormat[List[T]] = new BigQueryJsonFormat[List[T]] {
    def write(list: List[T]) = JsArray(list.map(_.toJson).toVector)
    def read(value: JsValue): List[T] = value match {
      case JsArray(elements) => elements.iterator.map(_.asJsObject.fields("v").convertTo[T]).toList
      case x => deserializationError("Expected List as JsArray, but got " + x)
    }
  }

  /**
   * Supplies the BigQueryJsonFormat for Arrays.
   */
  implicit def arrayFormat[T: BigQueryJsonFormat: ClassTag]: BigQueryJsonFormat[Array[T]] =
    new BigQueryJsonFormat[Array[T]] {
      def write(array: Array[T]) = JsArray(array.map(_.toJson).toVector)
      def read(value: JsValue) = value match {
        case JsArray(elements) => elements.map(_.asJsObject.fields("v").convertTo[T]).toArray[T]
        case x => deserializationError("Expected Array as JsArray, but got " + x)
      }
    }

  import collection.{immutable => imm}

  implicit def immIterableFormat[T: BigQueryJsonFormat]: BigQueryJsonFormat[immutable.Iterable[T]] = viaSeq[imm.Iterable[T], T](seq => imm.Iterable(seq: _*))
  implicit def immSeqFormat[T: BigQueryJsonFormat]: BigQueryJsonFormat[imm.Seq[T]] = viaSeq[imm.Seq[T], T](seq => imm.Seq(seq: _*))
  implicit def immIndexedSeqFormat[T: BigQueryJsonFormat]: BigQueryJsonFormat[imm.IndexedSeq[T]] = viaSeq[imm.IndexedSeq[T], T](seq => imm.IndexedSeq(seq: _*))
  implicit def immLinearSeqFormat[T: BigQueryJsonFormat]: BigQueryJsonFormat[imm.LinearSeq[T]] = viaSeq[imm.LinearSeq[T], T](seq => imm.LinearSeq(seq: _*))
  implicit def vectorFormat[T: BigQueryJsonFormat]: BigQueryJsonFormat[Vector[T]] = viaSeq[Vector[T], T](seq => Vector(seq: _*))


  implicit def iterableFormat[T: BigQueryJsonFormat]: BigQueryJsonFormat[collection.Iterable[T]] = viaSeq[collection.Iterable[T], T](seq => collection.Iterable(seq: _*))
  implicit def seqFormat[T: BigQueryJsonFormat]: BigQueryJsonFormat[collection.Seq[T]] = viaSeq[collection.Seq[T], T](seq => collection.Seq(seq: _*))
  implicit def indexedSeqFormat[T: BigQueryJsonFormat]: BigQueryJsonFormat[collection.IndexedSeq[T]] = viaSeq[collection.IndexedSeq[T], T](seq => collection.IndexedSeq(seq: _*))
  implicit def linearSeqFormat[T: BigQueryJsonFormat]: BigQueryJsonFormat[collection.LinearSeq[T]] = viaSeq[collection.LinearSeq[T], T](seq => collection.LinearSeq(seq: _*))

  /**
   * A BigQueryJsonFormat construction helper that creates a BigQueryJsonFormat for an Iterable type I from a builder function
   * Seq => I.
   */
  def viaSeq[I <: Iterable[T], T: BigQueryJsonFormat](f: imm.Seq[T] => I): BigQueryJsonFormat[I] =
    new BigQueryJsonFormat[I] {
      def write(iterable: I) = JsArray(iterable.map(_.toJson).toVector)
      def read(value: JsValue) = value match {
        case JsArray(elements) => f(elements.map(_.asJsObject.fields("v").convertTo[T]))
        case x => deserializationError("Expected Collection as JsArray, but got " + x)
      }
    }
}
