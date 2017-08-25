/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.neo4j.internal.codec

import org.neo4j.driver.v1.Value
import shapeless._
import shapeless.labelled._

import scala.util.{Failure, Success, Try}

trait Neo4jDecoder[A] {
  def decode(record: Value, fieldName: Symbol): Try[A]
}

object Neo4jDecoder {
  private def instance[A](f: (Value, Symbol) => Try[A]): Neo4jDecoder[A] =
    new Neo4jDecoder[A] {
      def decode(record: Value, fieldName: Symbol) = f(record, fieldName)
    }

  implicit val hnilDecoder: Neo4jDecoder[HNil] = instance((_, _) => Success(HNil))

  implicit def hlistDecoder[K <: Symbol, H, T <: HList](
      implicit witness: Witness.Aux[K],
      hDecoder: Lazy[Neo4jDecoder[H]],
      tDecoder: Lazy[Neo4jDecoder[T]]
  ): Neo4jDecoder[FieldType[K, H] :: T] = instance {
    case (reader, fieldName) => {
      val headField = hDecoder.value.decode(reader, witness.value)
      val tailFields = tDecoder.value.decode(reader, fieldName)
      (headField, tailFields) match {
        case (Success(h), Success(t)) => Success(field[K](h) :: t)
        case _ => Failure(null)
      }
    }
    case e => Failure(null)
  }

  implicit def objectDecoder[A, Repr <: HList](
      implicit gen: LabelledGeneric.Aux[A, Repr],
      hlistDecoder: Neo4jDecoder[Repr]
  ): Neo4jDecoder[A] = instance { (reader, fieldName) =>
    hlistDecoder.decode(reader, fieldName).map(gen.from)
  }

  def apply[A](implicit ev: Neo4jDecoder[A]): Neo4jDecoder[A] = ev

  implicit val booleanDecoder: Neo4jDecoder[Boolean] = instance { (record, fieldName) =>
    Success(record.get(fieldName.name, false))
  }
  implicit val stringDecoder: Neo4jDecoder[String] = instance { (record, fieldName) =>
    Success(record.get(fieldName.name, ""))
  }
  implicit val intDecoder: Neo4jDecoder[Int] = instance { (record, fieldName) =>
    Success(record.get(fieldName.name, 0))
  }
  implicit val longDecoder: Neo4jDecoder[Long] = instance { (record, fieldName) =>
    Success(record.get(fieldName.name, 0L))
  }

  implicit def optionDecoder[A](implicit aEncoder: Neo4jDecoder[A]): Neo4jDecoder[Option[A]] = instance {
    (record, fieldName) =>
      if (record.get(fieldName.name).isNull)
        Success(None)
      else
        aEncoder.decode(record, fieldName).map(Some(_))
  }
}
