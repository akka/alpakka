/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.neo4j.internal.codec

import shapeless.labelled.FieldType
import shapeless.ops.hlist.IsHCons
import shapeless.{::, HList, HNil, LabelledGeneric, Lazy, Witness}

trait Neo4jEncoder[A] {
  def encode(builder: StringBuilder, fieldName: Option[Symbol] = None, a: A, n: Int = 0): Int
}

object Neo4jEncoder {
  private def instance[A](f: (StringBuilder, Option[Symbol], A, Int) => Int) =
    new Neo4jEncoder[A] {
      override def encode(builder: StringBuilder, fieldName: Option[Symbol] = None, a: A, n: Int): Int =
        f(builder, fieldName, a, n)
    }

  implicit val hnilEncoder: Neo4jEncoder[HNil] = instance[HNil] { case _ => 0 }

  implicit def hlistEncoder[K <: Symbol, H, T <: shapeless.HList](
      implicit witness: Witness.Aux[K],
      isHCons: IsHCons.Aux[H :: T, H, T],
      hEncoder: Lazy[Neo4jEncoder[H]],
      tEncoder: Lazy[Neo4jEncoder[T]]
  ): Neo4jEncoder[FieldType[K, H] :: T] =
    instance[FieldType[K, H] :: T] {
      case (writer, fieldName, o, n) =>
        val inc = hEncoder.value.encode(writer, Some(witness.value), isHCons.head(o), n)
        tEncoder.value.encode(writer, fieldName, isHCons.tail(o), n + inc)
    }

  implicit def objectEncoder[A, Repr <: HList](
      implicit gen: LabelledGeneric.Aux[A, Repr],
      hlistEncoder: Lazy[Neo4jEncoder[Repr]]
  ): Neo4jEncoder[A] = instance {
    case (writer, fieldName, o, n) =>
      writer.append("{")
      hlistEncoder.value.encode(writer, fieldName, gen.to(o), 0)
      writer.append("}")
      1
  }

  def apply[A](implicit enc: Neo4jEncoder[A]): Neo4jEncoder[A] = enc

  /**
   * Prepend comma if necessary.
   *
   * @return number of field written
   */
  private def append[A](quoted: Boolean)(builder: StringBuilder, fieldName: Option[Symbol], a: A, n: Int) = {
    if (n > 0) builder.append(", ")
    fieldName
      .map(symbol => symbol.name)
      .map { name =>
        if (quoted)
          builder.append(s"$name: '$a'")
        else
          builder.append(s"$name: $a")
        1

      }
      .getOrElse(0)
  }

  implicit val booleanEncoder: Neo4jEncoder[Boolean] = instance(append(false))

  implicit val stringEncoder: Neo4jEncoder[String] = instance(append(true))

  implicit val longEncoder: Neo4jEncoder[Long] = instance(append(false))

  implicit val intEncoder: Neo4jEncoder[Int] = instance(append(false))

  implicit def optionEncoder[A](implicit aEncoder: Neo4jEncoder[A]): Neo4jEncoder[Option[A]] = instance {
    (builder, fieldName, b, n) =>
      b match {
        case Some(a) =>
          aEncoder.encode(builder, fieldName, a, n)
        case None => 0
      }

  }

}
