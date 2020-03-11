/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import java.util.Date

import akka.stream.alpakka.geode.AkkaPdxSerializer
import org.apache.geode.pdx.{PdxReader, PdxWriter}

//#person-pdx-serializer
object PersonPdxSerializer extends AkkaPdxSerializer[Person] {
  override def clazz: Class[Person] = classOf[Person]

  override def toData(o: scala.Any, out: PdxWriter): Boolean =
    if (o.isInstanceOf[Person]) {
      val p = o.asInstanceOf[Person]
      out.writeInt("id", p.id)
      out.writeString("name", p.name)
      out.writeDate("birthDate", p.birthDate)
      true
    } else
      false

  override def fromData(clazz: Class[_], in: PdxReader): AnyRef = {
    val id: Int = in.readInt("id")
    val name: String = in.readString("name")
    val birthDate: Date = in.readDate("birthDate")
    Person(id, name, birthDate)
  }
}
//#person-pdx-serializer
