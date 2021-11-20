/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.geode.impl.pdx

import java.util.{Date, UUID}

import akka.annotation.InternalApi
import org.apache.geode.pdx.PdxWriter
import shapeless.ops.hlist.IsHCons

@InternalApi
trait PdxEncoder[A] {
  def encode(writer: PdxWriter, a: A, fieldName: Symbol = null): Boolean
}

object PdxEncoder {

  import shapeless._
  import shapeless.labelled._

  private def instance[A](f: (PdxWriter, A, Symbol) => Boolean) =
    new PdxEncoder[A] {
      def encode(writer: PdxWriter, a: A, fieldName: Symbol = null): Boolean = f(writer, a, fieldName)
    }

  implicit val hnilEncoder: PdxEncoder[HNil] =
    instance[HNil] { case _ => true }

  implicit def hlistEncoder[K <: Symbol, H, T <: shapeless.HList](
      implicit witness: Witness.Aux[K],
      isHCons: IsHCons.Aux[H :: T, H, T],
      hEncoder: Lazy[PdxEncoder[H]],
      tEncoder: Lazy[PdxEncoder[T]]
  ): PdxEncoder[FieldType[K, H] :: T] =
    instance[FieldType[K, H] :: T] {
      case (writer, o, fieldName) =>
        hEncoder.value.encode(writer, isHCons.head(o), witness.value)
        tEncoder.value.encode(writer, isHCons.tail(o), fieldName)

    }

  implicit def objectEncoder[A, Repr <: HList](
      implicit gen: LabelledGeneric.Aux[A, Repr],
      hlistEncoder: Lazy[PdxEncoder[Repr]]
  ): PdxEncoder[A] = instance {
    case (writer, o, fieldName) =>
      hlistEncoder.value.encode(writer, gen.to(o), fieldName)
  }

  def apply[A](implicit enc: PdxEncoder[A]): PdxEncoder[A] = enc

  implicit def booleanEncoder: PdxEncoder[Boolean] = instance {
    case (writer: PdxWriter, b: Boolean, fieldName: Symbol) =>
      writer.writeBoolean(fieldName.name, b)
      true
    case _ => false
  }

  implicit def booleanListEncoder: PdxEncoder[List[Boolean]] = instance {
    case (writer: PdxWriter, bs: List[Boolean], fieldName: Symbol) =>
      writer.writeBooleanArray(fieldName.name, bs.toArray)
      true
    case _ => false
  }

  implicit def booleanArrayEncoder: PdxEncoder[Array[Boolean]] = instance {
    case (writer: PdxWriter, bs: Array[Boolean], fieldName: Symbol) =>
      writer.writeBooleanArray(fieldName.name, bs)
      true
    case _ => false
  }

  implicit def intEncoder: PdxEncoder[Int] = instance {
    case (writer: PdxWriter, i: Int, fieldName: Symbol) =>
      writer.writeInt(fieldName.name, i)
      true
    case _ => false
  }

  implicit def intListEncoder: PdxEncoder[List[Int]] = instance {
    case (writer: PdxWriter, is: List[Int], fieldName: Symbol) =>
      writer.writeIntArray(fieldName.name, is.toArray)
      true
    case _ => false
  }

  implicit def intArrayEncoder: PdxEncoder[Array[Int]] = instance {
    case (writer: PdxWriter, is: Array[Int], fieldName: Symbol) =>
      writer.writeIntArray(fieldName.name, is)
      true
    case _ => false
  }

  implicit def doubleEncoder: PdxEncoder[Double] = instance {
    case (writer: PdxWriter, d: Double, fieldName: Symbol) =>
      writer.writeDouble(fieldName.name, d)
      true
    case _ => false
  }

  implicit def doubleListEncoder: PdxEncoder[List[Double]] = instance {
    case (writer: PdxWriter, ds: List[Double], fieldName: Symbol) =>
      writer.writeDoubleArray(fieldName.name, ds.toArray)
      true
    case _ => false
  }
  implicit def doubleArrayEncoder: PdxEncoder[Array[Double]] = instance {
    case (writer: PdxWriter, ds: Array[Double], fieldName: Symbol) =>
      writer.writeDoubleArray(fieldName.name, ds)
      true
    case _ => false
  }

  implicit def floatEncoder: PdxEncoder[Float] = instance {
    case (writer: PdxWriter, f: Float, fieldName: Symbol) =>
      writer.writeFloat(fieldName.name, f)
      true
    case _ => false
  }

  implicit def floatListEncoder: PdxEncoder[List[Float]] = instance {
    case (writer: PdxWriter, fs: List[Float], fieldName: Symbol) =>
      writer.writeFloatArray(fieldName.name, fs.toArray)
      true
    case _ => false
  }
  implicit def floatArrayEncoder: PdxEncoder[Array[Float]] = instance {
    case (writer: PdxWriter, fs: Array[Float], fieldName: Symbol) =>
      writer.writeFloatArray(fieldName.name, fs)
      true
    case _ => false
  }

  implicit def longEncoder: PdxEncoder[Long] = instance {
    case (writer: PdxWriter, l: Long, fieldName: Symbol) =>
      writer.writeLong(fieldName.name, l)
      true
    case _ => false
  }

  implicit def longListEncoder: PdxEncoder[List[Long]] = instance {
    case (writer: PdxWriter, ls: List[Long], fieldName: Symbol) =>
      writer.writeLongArray(fieldName.name, ls.toArray)
      true
    case _ => false
  }
  implicit def longArrayEncoder: PdxEncoder[Array[Long]] = instance {
    case (writer: PdxWriter, ls: Array[Long], fieldName: Symbol) =>
      writer.writeLongArray(fieldName.name, ls)
      true
    case _ => false
  }

  implicit def dateEncoder: PdxEncoder[Date] = instance {
    case (writer: PdxWriter, d: Date, fieldName: Symbol) =>
      writer.writeDate(fieldName.name, d)
      true
    case _ => false
  }

  implicit def charEncoder: PdxEncoder[Char] =
    instance {
      case (writer: PdxWriter, c: Char, fieldName: Symbol) =>
        writer.writeChar(fieldName.name, c)
        true
      case _ => false
    }
  implicit def charListEncoder: PdxEncoder[List[Char]] =
    instance {
      case (writer: PdxWriter, cs: List[Char], fieldName: Symbol) =>
        writer.writeCharArray(fieldName.name, cs.toArray)
        true
      case _ => false
    }
  implicit def charArrayEncoder: PdxEncoder[Array[Char]] =
    instance {
      case (writer: PdxWriter, cs: Array[Char], fieldName: Symbol) =>
        writer.writeCharArray(fieldName.name, cs)
        true
      case _ => false
    }

  implicit def stringEncoder: PdxEncoder[String] =
    instance {
      case (writer: PdxWriter, str: String, fieldName: Symbol) =>
        writer.writeString(fieldName.name, str)
        true
      case _ => false
    }

  implicit def stringListEncoder: PdxEncoder[List[String]] =
    instance {
      case (writer: PdxWriter, strs: List[String], fieldName: Symbol) =>
        writer.writeStringArray(fieldName.name, strs.toArray)
        true
      case _ => false
    }
  implicit def stringArrayEncoder: PdxEncoder[Array[String]] =
    instance {
      case (writer: PdxWriter, strs: Array[String], fieldName: Symbol) =>
        writer.writeStringArray(fieldName.name, strs)
        true
      case _ => false
    }

  implicit def uuidEncoder: PdxEncoder[UUID] =
    instance {
      case (writer: PdxWriter, uuid: UUID, fieldName: Symbol) =>
        writer.writeString(fieldName.name, uuid.toString)
        true
      case _ => false
    }

  implicit def listEncoder[T <: AnyRef]: PdxEncoder[List[T]] = instance {
    case (writer: PdxWriter, list: List[T], fieldName: Symbol) =>
      writer.writeObjectArray(fieldName.name, list.toArray)
      true
    case _ => false
  }

  implicit def setEncoder[T <: AnyRef]: PdxEncoder[Set[T]] = instance {
    case (writer: PdxWriter, set: Set[T], fieldName: Symbol) =>
      writer.writeObjectArray(fieldName.name, set.toArray)
      true
    case _ => false
  }

}
