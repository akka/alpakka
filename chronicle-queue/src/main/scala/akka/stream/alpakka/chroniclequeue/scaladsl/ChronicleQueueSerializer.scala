/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

// ORIGINAL LICENCE
/*
 *  Copyright 2017 PayPal
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package akka.stream.alpakka.chroniclequeue.scaladsl

import akka.util.ByteString
import net.openhft.chronicle.wire.{WireIn, WireOut}

import scala.reflect._

import akka.stream.alpakka.chroniclequeue.impl.ChronicleQueueSerializer

object ChronicleQueueSerializer {

  def apply[T: ClassTag](): ChronicleQueueSerializer[T] = classTag[T] match {
    case t if classOf[ByteString] == t.runtimeClass =>
      new ByteStringSerializer().asInstanceOf[ChronicleQueueSerializer[T]]
    case t if classOf[AnyRef].isAssignableFrom(t.runtimeClass) =>
      new ObjectSerializer[T]
    case t if classOf[Long] == t.runtimeClass =>
      new LongSerializer().asInstanceOf[ChronicleQueueSerializer[T]]
    case t if classOf[Int] == t.runtimeClass =>
      new IntSerializer().asInstanceOf[ChronicleQueueSerializer[T]]
    case t if classOf[Short] == t.runtimeClass =>
      new ShortSerializer().asInstanceOf[ChronicleQueueSerializer[T]]
    case t if classOf[Byte] == t.runtimeClass =>
      new ByteSerializer().asInstanceOf[ChronicleQueueSerializer[T]]
    case t if classOf[Char] == t.runtimeClass =>
      new CharSerializer().asInstanceOf[ChronicleQueueSerializer[T]]
    case t if classOf[Double] == t.runtimeClass =>
      new DoubleSerializer().asInstanceOf[ChronicleQueueSerializer[T]]
    case t if classOf[Float] == t.runtimeClass =>
      new FloatSerializer().asInstanceOf[ChronicleQueueSerializer[T]]
    case t if classOf[Boolean] == t.runtimeClass =>
      new BooleanSerializer().asInstanceOf[ChronicleQueueSerializer[T]]
    case t => throw new ClassCastException("Unsupported Type: " + t)
  }
}

class ByteStringSerializer extends ChronicleQueueSerializer[ByteString] {

  def writeElement(element: ByteString, wire: WireOut) = {
    val bb = element.asByteBuffer
    val output = new Array[Byte](bb.remaining)
    bb.get(output)
    wire.write().bytes(output)
  }

  def readElement(wire: WireIn): Option[ByteString] =
    // TODO: wire.read() may need some optimization. It uses a StringBuilder underneath
    Option(wire.read().bytes).map(ByteString(_))
}

class ObjectSerializer[T: ClassTag] extends ChronicleQueueSerializer[T] {

  val clazz: Class[T] = classTag[T].runtimeClass.asInstanceOf[Class[T]]

  def readElement(wire: WireIn): Option[T] = Option(wire.read().`object`(clazz))

  def writeElement(element: T, wire: WireOut): Unit = wire.write().`object`(clazz, element)
}

class LongSerializer extends ChronicleQueueSerializer[Long] {

  def readElement(wire: WireIn): Option[Long] = Option(wire.read().int64)

  def writeElement(element: Long, wire: WireOut): Unit = wire.write().int64(element)
}

class IntSerializer extends ChronicleQueueSerializer[Int] {

  def readElement(wire: WireIn): Option[Int] = Option(wire.read().int32)

  def writeElement(element: Int, wire: WireOut): Unit = wire.write().int32(element)
}

class ShortSerializer extends ChronicleQueueSerializer[Short] {

  def readElement(wire: WireIn): Option[Short] = Option(wire.read().int16)

  def writeElement(element: Short, wire: WireOut): Unit = wire.write().int16(element)
}

class ByteSerializer extends ChronicleQueueSerializer[Byte] {

  def readElement(wire: WireIn): Option[Byte] = Option(wire.read().int8)

  def writeElement(element: Byte, wire: WireOut): Unit = wire.write().int8(element)
}

class CharSerializer extends ChronicleQueueSerializer[Char] {

  def readElement(wire: WireIn): Option[Char] = Option(wire.read().int16).map(_.toChar)

  def writeElement(element: Char, wire: WireOut): Unit = wire.write().int16(element.toShort)
}

class DoubleSerializer extends ChronicleQueueSerializer[Double] {

  def readElement(wire: WireIn): Option[Double] = Option(wire.read().float64())

  def writeElement(element: Double, wire: WireOut): Unit = wire.write().float64(element)
}

class FloatSerializer extends ChronicleQueueSerializer[Float] {

  def readElement(wire: WireIn): Option[Float] = Option(wire.read().float64).map(_.toFloat)

  def writeElement(element: Float, wire: WireOut): Unit = wire.write().float64(element)
}

class BooleanSerializer extends ChronicleQueueSerializer[Boolean] {

  def readElement(wire: WireIn): Option[Boolean] = Option(wire.read().bool())

  def writeElement(element: Boolean, wire: WireOut): Unit = wire.write().bool(element)
}
