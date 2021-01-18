/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.scaladsl.spray

import spray.json._

import scala.reflect.ClassTag

trait BigQueryCollectionFormats {

  /**
   * Supplies the JsonFormat for Lists.
   */
  implicit def listFormat[T: BigQueryJsonFormat]: BigQueryJsonFormat[List[T]] = new BigQueryJsonFormat[List[T]] {
    def write(list: List[T]) = JsArray(list.map(_.toJson).toVector)
    def read(value: JsValue): List[T] = value match {
      case JsArray(elements) => elements.iterator.map(_.asJsObject.fields("v").convertTo[T]).toList
      case x => deserializationError("Expected List as JsArray, but got " + x)
    }
  }

  /**
   * Supplies the JsonFormat for Arrays.
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

  implicit def immIterableFormat[T: BigQueryJsonFormat] = viaSeq[imm.Iterable[T], T](seq => imm.Iterable(seq: _*))
  implicit def immSeqFormat[T: BigQueryJsonFormat] = viaSeq[imm.Seq[T], T](seq => imm.Seq(seq: _*))
  implicit def immIndexedSeqFormat[T: BigQueryJsonFormat] = viaSeq[imm.IndexedSeq[T], T](seq => imm.IndexedSeq(seq: _*))
  implicit def immLinearSeqFormat[T: BigQueryJsonFormat] = viaSeq[imm.LinearSeq[T], T](seq => imm.LinearSeq(seq: _*))
  implicit def vectorFormat[T: BigQueryJsonFormat] = viaSeq[Vector[T], T](seq => Vector(seq: _*))

  import collection._

  implicit def iterableFormat[T: BigQueryJsonFormat] = viaSeq[Iterable[T], T](seq => Iterable(seq: _*))
  implicit def seqFormat[T: BigQueryJsonFormat] = viaSeq[Seq[T], T](seq => Seq(seq: _*))
  implicit def indexedSeqFormat[T: BigQueryJsonFormat] = viaSeq[IndexedSeq[T], T](seq => IndexedSeq(seq: _*))
  implicit def linearSeqFormat[T: BigQueryJsonFormat] = viaSeq[LinearSeq[T], T](seq => LinearSeq(seq: _*))

  /**
   * A JsonFormat construction helper that creates a JsonFormat for an Iterable type I from a builder function
   * List => I.
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
