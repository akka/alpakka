/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.geode.impl.pdx

import java.util.Properties

import akka.annotation.InternalApi
import org.apache.geode.cache.Declarable
import org.apache.geode.pdx.{PdxReader, PdxSerializer, PdxWriter}

import scala.reflect.ClassTag
import scala.util.Success

//#shapeless-pdx-serializer
@InternalApi
private[geode] class ShapelessPdxSerializer[A <: AnyRef](enc: PdxEncoder[A], dec: PdxDecoder[A])(
    implicit tag: ClassTag[A]
) extends PdxSerializer
    with Declarable {

  override def toData(o: scala.Any, out: PdxWriter): Boolean =
    tag.runtimeClass.isInstance(o) &&
    enc.encode(out, o.asInstanceOf[A])

  override def fromData(clazz: Class[_], in: PdxReader): A =
    dec.decode(in, null) match {
      case Success(e) => e
      case _ => null.asInstanceOf[A]
    }

  override def init(props: Properties): Unit = {}
}

//#shapeless-pdx-serializer
