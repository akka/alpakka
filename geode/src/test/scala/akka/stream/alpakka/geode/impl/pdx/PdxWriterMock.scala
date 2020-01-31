/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.geode.impl.pdx

import java.util.Date

import org.apache.geode.pdx.{PdxUnreadFields, PdxWriter}

object PdxMocks {

  class WriterMock extends PdxWriter {
    override def writeShortArray(fieldName: String, value: Array[Short]) = { println("Write value"); this }

    override def markIdentityField(fieldName: String) = { println(s"Write value"); this }

    override def writeString(fieldName: String, value: String) = { println(s"Write $value"); this }

    override def writeDate(fieldName: String, value: Date) = { println(s"Write $value"); this }

    override def writeFloat(fieldName: String, value: Float) = { println(s"Write $value"); this }

    override def writeCharArray(fieldName: String, value: Array[Char]) = { println(s"Write $value"); this }

    override def writeDouble(fieldName: String, value: Double) = { println(s"Write $value"); this }

    override def writeObjectArray(fieldName: String, value: Array[AnyRef]) = { println(s"Write $value"); this }

    override def writeObjectArray(fieldName: String, value: Array[AnyRef], checkPortability: Boolean) = {
      println(s"Write $value"); this
    }

    override def writeShort(fieldName: String, value: Short) = { println(s"Write $value"); this }

    override def writeArrayOfByteArrays(fieldName: String, value: Array[Array[Byte]]) = {
      println(s"Write $value"); this
    }

    override def writeField[CT, VT <: CT](fieldName: String, fieldValue: VT, fieldType: Class[CT]) = {
      println(s"Write $fieldName"); this
    }

    override def writeField[CT, VT <: CT](fieldName: String,
                                          fieldValue: VT,
                                          fieldType: Class[CT],
                                          checkPortability: Boolean) = { println(s"Write $fieldName"); this }

    override def writeInt(fieldName: String, value: Int) = { println(s"Write $value"); this }

    override def writeDoubleArray(fieldName: String, value: Array[Double]) = { println(s"Write $value"); this }

    override def writeLongArray(fieldName: String, value: Array[Long]) = { println(s"Write $value"); this }

    override def writeByteArray(fieldName: String, value: Array[Byte]) = { println(s"Write $value"); this }

    override def writeBooleanArray(fieldName: String, value: Array[Boolean]) = { println(s"Write $value"); this }

    override def writeIntArray(fieldName: String, value: Array[Int]) = { println(s"Write $value"); this }

    override def writeBoolean(fieldName: String, value: Boolean) = { println(s"Write $value"); this }

    override def writeStringArray(fieldName: String, value: Array[String]) = { println(s"Write $value"); this }

    override def writeObject(fieldName: String, value: scala.Any) = { println(s"Write $value"); this }

    override def writeObject(fieldName: String, value: scala.Any, checkPortability: Boolean) = {
      println(s"Write $value"); this
    }

    override def writeFloatArray(fieldName: String, value: Array[Float]) = { println(s"Write $value"); this }

    override def writeChar(fieldName: String, value: Char) = { println(s"Write $value"); this }

    override def writeLong(fieldName: String, value: Long) = { println(s"Write $value"); this }

    override def writeUnreadFields(unread: PdxUnreadFields) = { println(s"Write $unread"); this }

    override def writeByte(fieldName: String, value: Byte) = { println(s"Write $value"); this }
  }

  implicit val writerMock = new WriterMock()
}
