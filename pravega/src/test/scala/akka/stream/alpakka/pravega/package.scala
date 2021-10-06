/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka

import io.pravega.client.stream.Serializer
import java.nio.ByteBuffer
import io.pravega.client.stream.impl.UTF8StringSerializer

package object pravega {

  implicit val stringSerializer = new UTF8StringSerializer()

  implicit val personSerializer = new Serializer[Person] {
    def serialize(x: Person): ByteBuffer = {
      val name = x.firstname.getBytes("UTF-8")
      val buff = ByteBuffer.allocate(4 + name.length).putInt(x.id)
      buff.put(ByteBuffer.wrap(name))
      buff.position(0)
      buff
    }

    def deserialize(x: ByteBuffer): Person = {
      val i = x.getInt()
      val name = new String(x.array())
      Person(i, name)
    }

  }

  implicit val intSerializer = new Serializer[Int] {
    override def serialize(value: Int): ByteBuffer = {
      val buff = ByteBuffer.allocate(4).putInt(value)
      buff.position(0)
      buff
    }

    override def deserialize(serializedValue: ByteBuffer): Int =
      serializedValue.getInt
  }

}
