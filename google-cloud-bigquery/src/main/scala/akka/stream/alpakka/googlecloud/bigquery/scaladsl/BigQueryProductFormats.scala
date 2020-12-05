/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.scaladsl

import spray.json.{
  deserializationError,
  DeserializationException,
  JsArray,
  JsObject,
  JsValue,
  JsonFormat,
  JsonReader,
  ProductFormats,
  RootJsonFormat,
  StandardFormats
}

import scala.reflect.{classTag, ClassTag}

trait BigQueryProductFormats { this: ProductFormats with StandardFormats =>

  private type JF[T] = JsonFormat[T] // simple alias for reduced verbosity

  protected def fromBigQueryField[T](value: JsValue, fieldName: String, index: Int)(implicit reader: JsonReader[T]) =
    value match {
      case x: JsArray =>
        try reader.read(x.elements(index).asJsObject.fields("v"))
        catch {
          case e: IndexOutOfBoundsException =>
            deserializationError("Object is missing required member '" + fieldName + "'", e, fieldName :: Nil)
          case DeserializationException(msg, cause, fieldNames) =>
            deserializationError(msg, cause, fieldName :: fieldNames)
        }
      case _ => deserializationError("Object expected in field '" + fieldName + "'", fieldNames = fieldName :: Nil)
    }

  // Case classes with 1 parameters

  def bigQueryJsonFormat1[P1: JF, T <: Product: ClassTag](construct: (P1) => T): RootJsonFormat[T] = {
    val Array(p1) = extractFieldNames(classTag[T])
    bigQueryJsonFormat(construct, p1)
  }
  def bigQueryJsonFormat[P1: JF, T <: Product](construct: (P1) => T, fieldName1: String): RootJsonFormat[T] =
    new RootJsonFormat[T] {
      def write(p: T) = {
        val fields = new collection.mutable.ListBuffer[(String, JsValue)]
        fields.sizeHint(1 * 2)
        fields ++= productElement2Field[P1](fieldName1, p, 0)
        JsObject(fields.toSeq: _*)
      }
      def read(value: JsValue) = {
        val f = value.asJsObject.fields("f")
        val p1V = fromBigQueryField[P1](f, fieldName1, 0)
        construct(p1V)
      }
    }

  // Case classes with 2 parameters

  def bigQueryJsonFormat2[P1: JF, P2: JF, T <: Product: ClassTag](construct: (P1, P2) => T): RootJsonFormat[T] = {
    val Array(p1, p2) = extractFieldNames(classTag[T])
    bigQueryJsonFormat(construct, p1, p2)
  }
  def bigQueryJsonFormat[P1: JF, P2: JF, T <: Product](construct: (P1, P2) => T,
                                                       fieldName1: String,
                                                       fieldName2: String): RootJsonFormat[T] = new RootJsonFormat[T] {
    def write(p: T) = {
      val fields = new collection.mutable.ListBuffer[(String, JsValue)]
      fields.sizeHint(2 * 3)
      fields ++= productElement2Field[P1](fieldName1, p, 0)
      fields ++= productElement2Field[P2](fieldName2, p, 1)
      JsObject(fields.toSeq: _*)
    }
    def read(value: JsValue) = {
      val f = value.asJsObject.fields("f")
      val p1V = fromBigQueryField[P1](f, fieldName1, 0)
      val p2V = fromBigQueryField[P2](f, fieldName2, 1)
      construct(p1V, p2V)
    }
  }

  // Case classes with 3 parameters

  def bigQueryJsonFormat3[P1: JF, P2: JF, P3: JF, T <: Product: ClassTag](
      construct: (P1, P2, P3) => T
  ): RootJsonFormat[T] = {
    val Array(p1, p2, p3) = extractFieldNames(classTag[T])
    bigQueryJsonFormat(construct, p1, p2, p3)
  }
  def bigQueryJsonFormat[P1: JF, P2: JF, P3: JF, T <: Product](construct: (P1, P2, P3) => T,
                                                               fieldName1: String,
                                                               fieldName2: String,
                                                               fieldName3: String): RootJsonFormat[T] =
    new RootJsonFormat[T] {
      def write(p: T) = {
        val fields = new collection.mutable.ListBuffer[(String, JsValue)]
        fields.sizeHint(3 * 4)
        fields ++= productElement2Field[P1](fieldName1, p, 0)
        fields ++= productElement2Field[P2](fieldName2, p, 1)
        fields ++= productElement2Field[P3](fieldName3, p, 2)
        JsObject(fields.toSeq: _*)
      }
      def read(value: JsValue) = {
        val f = value.asJsObject.fields("f")
        val p1V = fromBigQueryField[P1](f, fieldName1, 0)
        val p2V = fromBigQueryField[P2](f, fieldName2, 1)
        val p3V = fromBigQueryField[P3](f, fieldName3, 2)
        construct(p1V, p2V, p3V)
      }
    }

  // Case classes with 4 parameters

  def bigQueryJsonFormat4[P1: JF, P2: JF, P3: JF, P4: JF, T <: Product: ClassTag](
      construct: (P1, P2, P3, P4) => T
  ): RootJsonFormat[T] = {
    val Array(p1, p2, p3, p4) = extractFieldNames(classTag[T])
    bigQueryJsonFormat(construct, p1, p2, p3, p4)
  }
  def bigQueryJsonFormat[P1: JF, P2: JF, P3: JF, P4: JF, T <: Product](construct: (P1, P2, P3, P4) => T,
                                                                       fieldName1: String,
                                                                       fieldName2: String,
                                                                       fieldName3: String,
                                                                       fieldName4: String): RootJsonFormat[T] =
    new RootJsonFormat[T] {
      def write(p: T) = {
        val fields = new collection.mutable.ListBuffer[(String, JsValue)]
        fields.sizeHint(4 * 5)
        fields ++= productElement2Field[P1](fieldName1, p, 0)
        fields ++= productElement2Field[P2](fieldName2, p, 1)
        fields ++= productElement2Field[P3](fieldName3, p, 2)
        fields ++= productElement2Field[P4](fieldName4, p, 3)
        JsObject(fields.toSeq: _*)
      }
      def read(value: JsValue) = {
        val f = value.asJsObject.fields("f")
        val p1V = fromBigQueryField[P1](f, fieldName1, 0)
        val p2V = fromBigQueryField[P2](f, fieldName2, 1)
        val p3V = fromBigQueryField[P3](f, fieldName3, 2)
        val p4V = fromBigQueryField[P4](f, fieldName4, 3)
        construct(p1V, p2V, p3V, p4V)
      }
    }

  // Case classes with 5 parameters

  def bigQueryJsonFormat5[P1: JF, P2: JF, P3: JF, P4: JF, P5: JF, T <: Product: ClassTag](
      construct: (P1, P2, P3, P4, P5) => T
  ): RootJsonFormat[T] = {
    val Array(p1, p2, p3, p4, p5) = extractFieldNames(classTag[T])
    bigQueryJsonFormat(construct, p1, p2, p3, p4, p5)
  }
  def bigQueryJsonFormat[P1: JF, P2: JF, P3: JF, P4: JF, P5: JF, T <: Product](construct: (P1, P2, P3, P4, P5) => T,
                                                                               fieldName1: String,
                                                                               fieldName2: String,
                                                                               fieldName3: String,
                                                                               fieldName4: String,
                                                                               fieldName5: String): RootJsonFormat[T] =
    new RootJsonFormat[T] {
      def write(p: T) = {
        val fields = new collection.mutable.ListBuffer[(String, JsValue)]
        fields.sizeHint(5 * 6)
        fields ++= productElement2Field[P1](fieldName1, p, 0)
        fields ++= productElement2Field[P2](fieldName2, p, 1)
        fields ++= productElement2Field[P3](fieldName3, p, 2)
        fields ++= productElement2Field[P4](fieldName4, p, 3)
        fields ++= productElement2Field[P5](fieldName5, p, 4)
        JsObject(fields.toSeq: _*)
      }
      def read(value: JsValue) = {
        val f = value.asJsObject.fields("f")
        val p1V = fromBigQueryField[P1](f, fieldName1, 0)
        val p2V = fromBigQueryField[P2](f, fieldName2, 1)
        val p3V = fromBigQueryField[P3](f, fieldName3, 2)
        val p4V = fromBigQueryField[P4](f, fieldName4, 3)
        val p5V = fromBigQueryField[P5](f, fieldName5, 4)
        construct(p1V, p2V, p3V, p4V, p5V)
      }
    }

  // Case classes with 6 parameters

  def bigQueryJsonFormat6[P1: JF, P2: JF, P3: JF, P4: JF, P5: JF, P6: JF, T <: Product: ClassTag](
      construct: (P1, P2, P3, P4, P5, P6) => T
  ): RootJsonFormat[T] = {
    val Array(p1, p2, p3, p4, p5, p6) = extractFieldNames(classTag[T])
    bigQueryJsonFormat(construct, p1, p2, p3, p4, p5, p6)
  }
  def bigQueryJsonFormat[P1: JF, P2: JF, P3: JF, P4: JF, P5: JF, P6: JF, T <: Product](
      construct: (P1, P2, P3, P4, P5, P6) => T,
      fieldName1: String,
      fieldName2: String,
      fieldName3: String,
      fieldName4: String,
      fieldName5: String,
      fieldName6: String
  ): RootJsonFormat[T] = new RootJsonFormat[T] {
    def write(p: T) = {
      val fields = new collection.mutable.ListBuffer[(String, JsValue)]
      fields.sizeHint(6 * 7)
      fields ++= productElement2Field[P1](fieldName1, p, 0)
      fields ++= productElement2Field[P2](fieldName2, p, 1)
      fields ++= productElement2Field[P3](fieldName3, p, 2)
      fields ++= productElement2Field[P4](fieldName4, p, 3)
      fields ++= productElement2Field[P5](fieldName5, p, 4)
      fields ++= productElement2Field[P6](fieldName6, p, 5)
      JsObject(fields.toSeq: _*)
    }
    def read(value: JsValue) = {
      val f = value.asJsObject.fields("f")
      val p1V = fromBigQueryField[P1](f, fieldName1, 0)
      val p2V = fromBigQueryField[P2](f, fieldName2, 1)
      val p3V = fromBigQueryField[P3](f, fieldName3, 2)
      val p4V = fromBigQueryField[P4](f, fieldName4, 3)
      val p5V = fromBigQueryField[P5](f, fieldName5, 4)
      val p6V = fromBigQueryField[P6](f, fieldName6, 5)
      construct(p1V, p2V, p3V, p4V, p5V, p6V)
    }
  }

  // Case classes with 7 parameters

  def bigQueryJsonFormat7[P1: JF, P2: JF, P3: JF, P4: JF, P5: JF, P6: JF, P7: JF, T <: Product: ClassTag](
      construct: (P1, P2, P3, P4, P5, P6, P7) => T
  ): RootJsonFormat[T] = {
    val Array(p1, p2, p3, p4, p5, p6, p7) = extractFieldNames(classTag[T])
    bigQueryJsonFormat(construct, p1, p2, p3, p4, p5, p6, p7)
  }
  def bigQueryJsonFormat[P1: JF, P2: JF, P3: JF, P4: JF, P5: JF, P6: JF, P7: JF, T <: Product](
      construct: (P1, P2, P3, P4, P5, P6, P7) => T,
      fieldName1: String,
      fieldName2: String,
      fieldName3: String,
      fieldName4: String,
      fieldName5: String,
      fieldName6: String,
      fieldName7: String
  ): RootJsonFormat[T] = new RootJsonFormat[T] {
    def write(p: T) = {
      val fields = new collection.mutable.ListBuffer[(String, JsValue)]
      fields.sizeHint(7 * 8)
      fields ++= productElement2Field[P1](fieldName1, p, 0)
      fields ++= productElement2Field[P2](fieldName2, p, 1)
      fields ++= productElement2Field[P3](fieldName3, p, 2)
      fields ++= productElement2Field[P4](fieldName4, p, 3)
      fields ++= productElement2Field[P5](fieldName5, p, 4)
      fields ++= productElement2Field[P6](fieldName6, p, 5)
      fields ++= productElement2Field[P7](fieldName7, p, 6)
      JsObject(fields.toSeq: _*)
    }
    def read(value: JsValue) = {
      val f = value.asJsObject.fields("f")
      val p1V = fromBigQueryField[P1](f, fieldName1, 0)
      val p2V = fromBigQueryField[P2](f, fieldName2, 1)
      val p3V = fromBigQueryField[P3](f, fieldName3, 2)
      val p4V = fromBigQueryField[P4](f, fieldName4, 3)
      val p5V = fromBigQueryField[P5](f, fieldName5, 4)
      val p6V = fromBigQueryField[P6](f, fieldName6, 5)
      val p7V = fromBigQueryField[P7](f, fieldName7, 6)
      construct(p1V, p2V, p3V, p4V, p5V, p6V, p7V)
    }
  }

  // Case classes with 8 parameters

  def bigQueryJsonFormat8[P1: JF, P2: JF, P3: JF, P4: JF, P5: JF, P6: JF, P7: JF, P8: JF, T <: Product: ClassTag](
      construct: (P1, P2, P3, P4, P5, P6, P7, P8) => T
  ): RootJsonFormat[T] = {
    val Array(p1, p2, p3, p4, p5, p6, p7, p8) = extractFieldNames(classTag[T])
    bigQueryJsonFormat(construct, p1, p2, p3, p4, p5, p6, p7, p8)
  }
  def bigQueryJsonFormat[P1: JF, P2: JF, P3: JF, P4: JF, P5: JF, P6: JF, P7: JF, P8: JF, T <: Product](
      construct: (P1, P2, P3, P4, P5, P6, P7, P8) => T,
      fieldName1: String,
      fieldName2: String,
      fieldName3: String,
      fieldName4: String,
      fieldName5: String,
      fieldName6: String,
      fieldName7: String,
      fieldName8: String
  ): RootJsonFormat[T] = new RootJsonFormat[T] {
    def write(p: T) = {
      val fields = new collection.mutable.ListBuffer[(String, JsValue)]
      fields.sizeHint(8 * 9)
      fields ++= productElement2Field[P1](fieldName1, p, 0)
      fields ++= productElement2Field[P2](fieldName2, p, 1)
      fields ++= productElement2Field[P3](fieldName3, p, 2)
      fields ++= productElement2Field[P4](fieldName4, p, 3)
      fields ++= productElement2Field[P5](fieldName5, p, 4)
      fields ++= productElement2Field[P6](fieldName6, p, 5)
      fields ++= productElement2Field[P7](fieldName7, p, 6)
      fields ++= productElement2Field[P8](fieldName8, p, 7)
      JsObject(fields.toSeq: _*)
    }
    def read(value: JsValue) = {
      val f = value.asJsObject.fields("f")
      val p1V = fromBigQueryField[P1](f, fieldName1, 0)
      val p2V = fromBigQueryField[P2](f, fieldName2, 1)
      val p3V = fromBigQueryField[P3](f, fieldName3, 2)
      val p4V = fromBigQueryField[P4](f, fieldName4, 3)
      val p5V = fromBigQueryField[P5](f, fieldName5, 4)
      val p6V = fromBigQueryField[P6](f, fieldName6, 5)
      val p7V = fromBigQueryField[P7](f, fieldName7, 6)
      val p8V = fromBigQueryField[P8](f, fieldName8, 7)
      construct(p1V, p2V, p3V, p4V, p5V, p6V, p7V, p8V)
    }
  }

  // Case classes with 9 parameters

  def bigQueryJsonFormat9[P1: JF,
                          P2: JF,
                          P3: JF,
                          P4: JF,
                          P5: JF,
                          P6: JF,
                          P7: JF,
                          P8: JF,
                          P9: JF,
                          T <: Product: ClassTag](
      construct: (P1, P2, P3, P4, P5, P6, P7, P8, P9) => T
  ): RootJsonFormat[T] = {
    val Array(p1, p2, p3, p4, p5, p6, p7, p8, p9) = extractFieldNames(classTag[T])
    bigQueryJsonFormat(construct, p1, p2, p3, p4, p5, p6, p7, p8, p9)
  }
  def bigQueryJsonFormat[P1: JF, P2: JF, P3: JF, P4: JF, P5: JF, P6: JF, P7: JF, P8: JF, P9: JF, T <: Product](
      construct: (P1, P2, P3, P4, P5, P6, P7, P8, P9) => T,
      fieldName1: String,
      fieldName2: String,
      fieldName3: String,
      fieldName4: String,
      fieldName5: String,
      fieldName6: String,
      fieldName7: String,
      fieldName8: String,
      fieldName9: String
  ): RootJsonFormat[T] = new RootJsonFormat[T] {
    def write(p: T) = {
      val fields = new collection.mutable.ListBuffer[(String, JsValue)]
      fields.sizeHint(9 * 10)
      fields ++= productElement2Field[P1](fieldName1, p, 0)
      fields ++= productElement2Field[P2](fieldName2, p, 1)
      fields ++= productElement2Field[P3](fieldName3, p, 2)
      fields ++= productElement2Field[P4](fieldName4, p, 3)
      fields ++= productElement2Field[P5](fieldName5, p, 4)
      fields ++= productElement2Field[P6](fieldName6, p, 5)
      fields ++= productElement2Field[P7](fieldName7, p, 6)
      fields ++= productElement2Field[P8](fieldName8, p, 7)
      fields ++= productElement2Field[P9](fieldName9, p, 8)
      JsObject(fields.toSeq: _*)
    }
    def read(value: JsValue) = {
      val f = value.asJsObject.fields("f")
      val p1V = fromBigQueryField[P1](f, fieldName1, 0)
      val p2V = fromBigQueryField[P2](f, fieldName2, 1)
      val p3V = fromBigQueryField[P3](f, fieldName3, 2)
      val p4V = fromBigQueryField[P4](f, fieldName4, 3)
      val p5V = fromBigQueryField[P5](f, fieldName5, 4)
      val p6V = fromBigQueryField[P6](f, fieldName6, 5)
      val p7V = fromBigQueryField[P7](f, fieldName7, 6)
      val p8V = fromBigQueryField[P8](f, fieldName8, 7)
      val p9V = fromBigQueryField[P9](f, fieldName9, 8)
      construct(p1V, p2V, p3V, p4V, p5V, p6V, p7V, p8V, p9V)
    }
  }

  // Case classes with 10 parameters

  def bigQueryJsonFormat10[P1: JF,
                           P2: JF,
                           P3: JF,
                           P4: JF,
                           P5: JF,
                           P6: JF,
                           P7: JF,
                           P8: JF,
                           P9: JF,
                           P10: JF,
                           T <: Product: ClassTag](
      construct: (P1, P2, P3, P4, P5, P6, P7, P8, P9, P10) => T
  ): RootJsonFormat[T] = {
    val Array(p1, p2, p3, p4, p5, p6, p7, p8, p9, p10) = extractFieldNames(classTag[T])
    bigQueryJsonFormat(construct, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10)
  }
  def bigQueryJsonFormat[P1: JF, P2: JF, P3: JF, P4: JF, P5: JF, P6: JF, P7: JF, P8: JF, P9: JF, P10: JF, T <: Product](
      construct: (P1, P2, P3, P4, P5, P6, P7, P8, P9, P10) => T,
      fieldName1: String,
      fieldName2: String,
      fieldName3: String,
      fieldName4: String,
      fieldName5: String,
      fieldName6: String,
      fieldName7: String,
      fieldName8: String,
      fieldName9: String,
      fieldName10: String
  ): RootJsonFormat[T] = new RootJsonFormat[T] {
    def write(p: T) = {
      val fields = new collection.mutable.ListBuffer[(String, JsValue)]
      fields.sizeHint(10 * 11)
      fields ++= productElement2Field[P1](fieldName1, p, 0)
      fields ++= productElement2Field[P2](fieldName2, p, 1)
      fields ++= productElement2Field[P3](fieldName3, p, 2)
      fields ++= productElement2Field[P4](fieldName4, p, 3)
      fields ++= productElement2Field[P5](fieldName5, p, 4)
      fields ++= productElement2Field[P6](fieldName6, p, 5)
      fields ++= productElement2Field[P7](fieldName7, p, 6)
      fields ++= productElement2Field[P8](fieldName8, p, 7)
      fields ++= productElement2Field[P9](fieldName9, p, 8)
      fields ++= productElement2Field[P10](fieldName10, p, 9)
      JsObject(fields.toSeq: _*)
    }
    def read(value: JsValue) = {
      val f = value.asJsObject.fields("f")
      val p1V = fromBigQueryField[P1](f, fieldName1, 0)
      val p2V = fromBigQueryField[P2](f, fieldName2, 1)
      val p3V = fromBigQueryField[P3](f, fieldName3, 2)
      val p4V = fromBigQueryField[P4](f, fieldName4, 3)
      val p5V = fromBigQueryField[P5](f, fieldName5, 4)
      val p6V = fromBigQueryField[P6](f, fieldName6, 5)
      val p7V = fromBigQueryField[P7](f, fieldName7, 6)
      val p8V = fromBigQueryField[P8](f, fieldName8, 7)
      val p9V = fromBigQueryField[P9](f, fieldName9, 8)
      val p10V = fromBigQueryField[P10](f, fieldName10, 9)
      construct(p1V, p2V, p3V, p4V, p5V, p6V, p7V, p8V, p9V, p10V)
    }
  }

  // Case classes with 11 parameters

  def bigQueryJsonFormat11[P1: JF,
                           P2: JF,
                           P3: JF,
                           P4: JF,
                           P5: JF,
                           P6: JF,
                           P7: JF,
                           P8: JF,
                           P9: JF,
                           P10: JF,
                           P11: JF,
                           T <: Product: ClassTag](
      construct: (P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11) => T
  ): RootJsonFormat[T] = {
    val Array(p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11) = extractFieldNames(classTag[T])
    bigQueryJsonFormat(construct, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11)
  }
  def bigQueryJsonFormat[P1: JF,
                         P2: JF,
                         P3: JF,
                         P4: JF,
                         P5: JF,
                         P6: JF,
                         P7: JF,
                         P8: JF,
                         P9: JF,
                         P10: JF,
                         P11: JF,
                         T <: Product](construct: (P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11) => T,
                                       fieldName1: String,
                                       fieldName2: String,
                                       fieldName3: String,
                                       fieldName4: String,
                                       fieldName5: String,
                                       fieldName6: String,
                                       fieldName7: String,
                                       fieldName8: String,
                                       fieldName9: String,
                                       fieldName10: String,
                                       fieldName11: String): RootJsonFormat[T] = new RootJsonFormat[T] {
    def write(p: T) = {
      val fields = new collection.mutable.ListBuffer[(String, JsValue)]
      fields.sizeHint(11 * 12)
      fields ++= productElement2Field[P1](fieldName1, p, 0)
      fields ++= productElement2Field[P2](fieldName2, p, 1)
      fields ++= productElement2Field[P3](fieldName3, p, 2)
      fields ++= productElement2Field[P4](fieldName4, p, 3)
      fields ++= productElement2Field[P5](fieldName5, p, 4)
      fields ++= productElement2Field[P6](fieldName6, p, 5)
      fields ++= productElement2Field[P7](fieldName7, p, 6)
      fields ++= productElement2Field[P8](fieldName8, p, 7)
      fields ++= productElement2Field[P9](fieldName9, p, 8)
      fields ++= productElement2Field[P10](fieldName10, p, 9)
      fields ++= productElement2Field[P11](fieldName11, p, 10)
      JsObject(fields.toSeq: _*)
    }
    def read(value: JsValue) = {
      val f = value.asJsObject.fields("f")
      val p1V = fromBigQueryField[P1](f, fieldName1, 0)
      val p2V = fromBigQueryField[P2](f, fieldName2, 1)
      val p3V = fromBigQueryField[P3](f, fieldName3, 2)
      val p4V = fromBigQueryField[P4](f, fieldName4, 3)
      val p5V = fromBigQueryField[P5](f, fieldName5, 4)
      val p6V = fromBigQueryField[P6](f, fieldName6, 5)
      val p7V = fromBigQueryField[P7](f, fieldName7, 6)
      val p8V = fromBigQueryField[P8](f, fieldName8, 7)
      val p9V = fromBigQueryField[P9](f, fieldName9, 8)
      val p10V = fromBigQueryField[P10](f, fieldName10, 9)
      val p11V = fromBigQueryField[P11](f, fieldName11, 10)
      construct(p1V, p2V, p3V, p4V, p5V, p6V, p7V, p8V, p9V, p10V, p11V)
    }
  }

  // Case classes with 12 parameters

  def bigQueryJsonFormat12[P1: JF,
                           P2: JF,
                           P3: JF,
                           P4: JF,
                           P5: JF,
                           P6: JF,
                           P7: JF,
                           P8: JF,
                           P9: JF,
                           P10: JF,
                           P11: JF,
                           P12: JF,
                           T <: Product: ClassTag](
      construct: (P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12) => T
  ): RootJsonFormat[T] = {
    val Array(p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12) = extractFieldNames(classTag[T])
    bigQueryJsonFormat(construct, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12)
  }
  def bigQueryJsonFormat[P1: JF,
                         P2: JF,
                         P3: JF,
                         P4: JF,
                         P5: JF,
                         P6: JF,
                         P7: JF,
                         P8: JF,
                         P9: JF,
                         P10: JF,
                         P11: JF,
                         P12: JF,
                         T <: Product](construct: (P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12) => T,
                                       fieldName1: String,
                                       fieldName2: String,
                                       fieldName3: String,
                                       fieldName4: String,
                                       fieldName5: String,
                                       fieldName6: String,
                                       fieldName7: String,
                                       fieldName8: String,
                                       fieldName9: String,
                                       fieldName10: String,
                                       fieldName11: String,
                                       fieldName12: String): RootJsonFormat[T] = new RootJsonFormat[T] {
    def write(p: T) = {
      val fields = new collection.mutable.ListBuffer[(String, JsValue)]
      fields.sizeHint(12 * 13)
      fields ++= productElement2Field[P1](fieldName1, p, 0)
      fields ++= productElement2Field[P2](fieldName2, p, 1)
      fields ++= productElement2Field[P3](fieldName3, p, 2)
      fields ++= productElement2Field[P4](fieldName4, p, 3)
      fields ++= productElement2Field[P5](fieldName5, p, 4)
      fields ++= productElement2Field[P6](fieldName6, p, 5)
      fields ++= productElement2Field[P7](fieldName7, p, 6)
      fields ++= productElement2Field[P8](fieldName8, p, 7)
      fields ++= productElement2Field[P9](fieldName9, p, 8)
      fields ++= productElement2Field[P10](fieldName10, p, 9)
      fields ++= productElement2Field[P11](fieldName11, p, 10)
      fields ++= productElement2Field[P12](fieldName12, p, 11)
      JsObject(fields.toSeq: _*)
    }
    def read(value: JsValue) = {
      val f = value.asJsObject.fields("f")
      val p1V = fromBigQueryField[P1](f, fieldName1, 0)
      val p2V = fromBigQueryField[P2](f, fieldName2, 1)
      val p3V = fromBigQueryField[P3](f, fieldName3, 2)
      val p4V = fromBigQueryField[P4](f, fieldName4, 3)
      val p5V = fromBigQueryField[P5](f, fieldName5, 4)
      val p6V = fromBigQueryField[P6](f, fieldName6, 5)
      val p7V = fromBigQueryField[P7](f, fieldName7, 6)
      val p8V = fromBigQueryField[P8](f, fieldName8, 7)
      val p9V = fromBigQueryField[P9](f, fieldName9, 8)
      val p10V = fromBigQueryField[P10](f, fieldName10, 9)
      val p11V = fromBigQueryField[P11](f, fieldName11, 10)
      val p12V = fromBigQueryField[P12](f, fieldName12, 11)
      construct(p1V, p2V, p3V, p4V, p5V, p6V, p7V, p8V, p9V, p10V, p11V, p12V)
    }
  }

  // Case classes with 13 parameters

  def bigQueryJsonFormat13[P1: JF,
                           P2: JF,
                           P3: JF,
                           P4: JF,
                           P5: JF,
                           P6: JF,
                           P7: JF,
                           P8: JF,
                           P9: JF,
                           P10: JF,
                           P11: JF,
                           P12: JF,
                           P13: JF,
                           T <: Product: ClassTag](
      construct: (P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13) => T
  ): RootJsonFormat[T] = {
    val Array(p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13) = extractFieldNames(classTag[T])
    bigQueryJsonFormat(construct, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13)
  }
  def bigQueryJsonFormat[P1: JF,
                         P2: JF,
                         P3: JF,
                         P4: JF,
                         P5: JF,
                         P6: JF,
                         P7: JF,
                         P8: JF,
                         P9: JF,
                         P10: JF,
                         P11: JF,
                         P12: JF,
                         P13: JF,
                         T <: Product](construct: (P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13) => T,
                                       fieldName1: String,
                                       fieldName2: String,
                                       fieldName3: String,
                                       fieldName4: String,
                                       fieldName5: String,
                                       fieldName6: String,
                                       fieldName7: String,
                                       fieldName8: String,
                                       fieldName9: String,
                                       fieldName10: String,
                                       fieldName11: String,
                                       fieldName12: String,
                                       fieldName13: String): RootJsonFormat[T] = new RootJsonFormat[T] {
    def write(p: T) = {
      val fields = new collection.mutable.ListBuffer[(String, JsValue)]
      fields.sizeHint(13 * 14)
      fields ++= productElement2Field[P1](fieldName1, p, 0)
      fields ++= productElement2Field[P2](fieldName2, p, 1)
      fields ++= productElement2Field[P3](fieldName3, p, 2)
      fields ++= productElement2Field[P4](fieldName4, p, 3)
      fields ++= productElement2Field[P5](fieldName5, p, 4)
      fields ++= productElement2Field[P6](fieldName6, p, 5)
      fields ++= productElement2Field[P7](fieldName7, p, 6)
      fields ++= productElement2Field[P8](fieldName8, p, 7)
      fields ++= productElement2Field[P9](fieldName9, p, 8)
      fields ++= productElement2Field[P10](fieldName10, p, 9)
      fields ++= productElement2Field[P11](fieldName11, p, 10)
      fields ++= productElement2Field[P12](fieldName12, p, 11)
      fields ++= productElement2Field[P13](fieldName13, p, 12)
      JsObject(fields.toSeq: _*)
    }
    def read(value: JsValue) = {
      val f = value.asJsObject.fields("f")
      val p1V = fromBigQueryField[P1](f, fieldName1, 0)
      val p2V = fromBigQueryField[P2](f, fieldName2, 1)
      val p3V = fromBigQueryField[P3](f, fieldName3, 2)
      val p4V = fromBigQueryField[P4](f, fieldName4, 3)
      val p5V = fromBigQueryField[P5](f, fieldName5, 4)
      val p6V = fromBigQueryField[P6](f, fieldName6, 5)
      val p7V = fromBigQueryField[P7](f, fieldName7, 6)
      val p8V = fromBigQueryField[P8](f, fieldName8, 7)
      val p9V = fromBigQueryField[P9](f, fieldName9, 8)
      val p10V = fromBigQueryField[P10](f, fieldName10, 9)
      val p11V = fromBigQueryField[P11](f, fieldName11, 10)
      val p12V = fromBigQueryField[P12](f, fieldName12, 11)
      val p13V = fromBigQueryField[P13](f, fieldName13, 12)
      construct(p1V, p2V, p3V, p4V, p5V, p6V, p7V, p8V, p9V, p10V, p11V, p12V, p13V)
    }
  }

  // Case classes with 14 parameters

  def bigQueryJsonFormat14[P1: JF,
                           P2: JF,
                           P3: JF,
                           P4: JF,
                           P5: JF,
                           P6: JF,
                           P7: JF,
                           P8: JF,
                           P9: JF,
                           P10: JF,
                           P11: JF,
                           P12: JF,
                           P13: JF,
                           P14: JF,
                           T <: Product: ClassTag](
      construct: (P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14) => T
  ): RootJsonFormat[T] = {
    val Array(p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14) = extractFieldNames(classTag[T])
    bigQueryJsonFormat(construct, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14)
  }
  def bigQueryJsonFormat[P1: JF,
                         P2: JF,
                         P3: JF,
                         P4: JF,
                         P5: JF,
                         P6: JF,
                         P7: JF,
                         P8: JF,
                         P9: JF,
                         P10: JF,
                         P11: JF,
                         P12: JF,
                         P13: JF,
                         P14: JF,
                         T <: Product](construct: (P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14) => T,
                                       fieldName1: String,
                                       fieldName2: String,
                                       fieldName3: String,
                                       fieldName4: String,
                                       fieldName5: String,
                                       fieldName6: String,
                                       fieldName7: String,
                                       fieldName8: String,
                                       fieldName9: String,
                                       fieldName10: String,
                                       fieldName11: String,
                                       fieldName12: String,
                                       fieldName13: String,
                                       fieldName14: String): RootJsonFormat[T] = new RootJsonFormat[T] {
    def write(p: T) = {
      val fields = new collection.mutable.ListBuffer[(String, JsValue)]
      fields.sizeHint(14 * 15)
      fields ++= productElement2Field[P1](fieldName1, p, 0)
      fields ++= productElement2Field[P2](fieldName2, p, 1)
      fields ++= productElement2Field[P3](fieldName3, p, 2)
      fields ++= productElement2Field[P4](fieldName4, p, 3)
      fields ++= productElement2Field[P5](fieldName5, p, 4)
      fields ++= productElement2Field[P6](fieldName6, p, 5)
      fields ++= productElement2Field[P7](fieldName7, p, 6)
      fields ++= productElement2Field[P8](fieldName8, p, 7)
      fields ++= productElement2Field[P9](fieldName9, p, 8)
      fields ++= productElement2Field[P10](fieldName10, p, 9)
      fields ++= productElement2Field[P11](fieldName11, p, 10)
      fields ++= productElement2Field[P12](fieldName12, p, 11)
      fields ++= productElement2Field[P13](fieldName13, p, 12)
      fields ++= productElement2Field[P14](fieldName14, p, 13)
      JsObject(fields.toSeq: _*)
    }
    def read(value: JsValue) = {
      val f = value.asJsObject.fields("f")
      val p1V = fromBigQueryField[P1](f, fieldName1, 0)
      val p2V = fromBigQueryField[P2](f, fieldName2, 1)
      val p3V = fromBigQueryField[P3](f, fieldName3, 2)
      val p4V = fromBigQueryField[P4](f, fieldName4, 3)
      val p5V = fromBigQueryField[P5](f, fieldName5, 4)
      val p6V = fromBigQueryField[P6](f, fieldName6, 5)
      val p7V = fromBigQueryField[P7](f, fieldName7, 6)
      val p8V = fromBigQueryField[P8](f, fieldName8, 7)
      val p9V = fromBigQueryField[P9](f, fieldName9, 8)
      val p10V = fromBigQueryField[P10](f, fieldName10, 9)
      val p11V = fromBigQueryField[P11](f, fieldName11, 10)
      val p12V = fromBigQueryField[P12](f, fieldName12, 11)
      val p13V = fromBigQueryField[P13](f, fieldName13, 12)
      val p14V = fromBigQueryField[P14](f, fieldName14, 13)
      construct(p1V, p2V, p3V, p4V, p5V, p6V, p7V, p8V, p9V, p10V, p11V, p12V, p13V, p14V)
    }
  }

  // Case classes with 15 parameters

  def bigQueryJsonFormat15[P1: JF,
                           P2: JF,
                           P3: JF,
                           P4: JF,
                           P5: JF,
                           P6: JF,
                           P7: JF,
                           P8: JF,
                           P9: JF,
                           P10: JF,
                           P11: JF,
                           P12: JF,
                           P13: JF,
                           P14: JF,
                           P15: JF,
                           T <: Product: ClassTag](
      construct: (P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15) => T
  ): RootJsonFormat[T] = {
    val Array(p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15) = extractFieldNames(classTag[T])
    bigQueryJsonFormat(construct, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15)
  }
  def bigQueryJsonFormat[P1: JF,
                         P2: JF,
                         P3: JF,
                         P4: JF,
                         P5: JF,
                         P6: JF,
                         P7: JF,
                         P8: JF,
                         P9: JF,
                         P10: JF,
                         P11: JF,
                         P12: JF,
                         P13: JF,
                         P14: JF,
                         P15: JF,
                         T <: Product](
      construct: (P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15) => T,
      fieldName1: String,
      fieldName2: String,
      fieldName3: String,
      fieldName4: String,
      fieldName5: String,
      fieldName6: String,
      fieldName7: String,
      fieldName8: String,
      fieldName9: String,
      fieldName10: String,
      fieldName11: String,
      fieldName12: String,
      fieldName13: String,
      fieldName14: String,
      fieldName15: String
  ): RootJsonFormat[T] = new RootJsonFormat[T] {
    def write(p: T) = {
      val fields = new collection.mutable.ListBuffer[(String, JsValue)]
      fields.sizeHint(15 * 16)
      fields ++= productElement2Field[P1](fieldName1, p, 0)
      fields ++= productElement2Field[P2](fieldName2, p, 1)
      fields ++= productElement2Field[P3](fieldName3, p, 2)
      fields ++= productElement2Field[P4](fieldName4, p, 3)
      fields ++= productElement2Field[P5](fieldName5, p, 4)
      fields ++= productElement2Field[P6](fieldName6, p, 5)
      fields ++= productElement2Field[P7](fieldName7, p, 6)
      fields ++= productElement2Field[P8](fieldName8, p, 7)
      fields ++= productElement2Field[P9](fieldName9, p, 8)
      fields ++= productElement2Field[P10](fieldName10, p, 9)
      fields ++= productElement2Field[P11](fieldName11, p, 10)
      fields ++= productElement2Field[P12](fieldName12, p, 11)
      fields ++= productElement2Field[P13](fieldName13, p, 12)
      fields ++= productElement2Field[P14](fieldName14, p, 13)
      fields ++= productElement2Field[P15](fieldName15, p, 14)
      JsObject(fields.toSeq: _*)
    }
    def read(value: JsValue) = {
      val f = value.asJsObject.fields("f")
      val p1V = fromBigQueryField[P1](f, fieldName1, 0)
      val p2V = fromBigQueryField[P2](f, fieldName2, 1)
      val p3V = fromBigQueryField[P3](f, fieldName3, 2)
      val p4V = fromBigQueryField[P4](f, fieldName4, 3)
      val p5V = fromBigQueryField[P5](f, fieldName5, 4)
      val p6V = fromBigQueryField[P6](f, fieldName6, 5)
      val p7V = fromBigQueryField[P7](f, fieldName7, 6)
      val p8V = fromBigQueryField[P8](f, fieldName8, 7)
      val p9V = fromBigQueryField[P9](f, fieldName9, 8)
      val p10V = fromBigQueryField[P10](f, fieldName10, 9)
      val p11V = fromBigQueryField[P11](f, fieldName11, 10)
      val p12V = fromBigQueryField[P12](f, fieldName12, 11)
      val p13V = fromBigQueryField[P13](f, fieldName13, 12)
      val p14V = fromBigQueryField[P14](f, fieldName14, 13)
      val p15V = fromBigQueryField[P15](f, fieldName15, 14)
      construct(p1V, p2V, p3V, p4V, p5V, p6V, p7V, p8V, p9V, p10V, p11V, p12V, p13V, p14V, p15V)
    }
  }

  // Case classes with 16 parameters

  def bigQueryJsonFormat16[P1: JF,
                           P2: JF,
                           P3: JF,
                           P4: JF,
                           P5: JF,
                           P6: JF,
                           P7: JF,
                           P8: JF,
                           P9: JF,
                           P10: JF,
                           P11: JF,
                           P12: JF,
                           P13: JF,
                           P14: JF,
                           P15: JF,
                           P16: JF,
                           T <: Product: ClassTag](
      construct: (P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16) => T
  ): RootJsonFormat[T] = {
    val Array(p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16) = extractFieldNames(classTag[T])
    bigQueryJsonFormat(construct, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16)
  }
  def bigQueryJsonFormat[P1: JF,
                         P2: JF,
                         P3: JF,
                         P4: JF,
                         P5: JF,
                         P6: JF,
                         P7: JF,
                         P8: JF,
                         P9: JF,
                         P10: JF,
                         P11: JF,
                         P12: JF,
                         P13: JF,
                         P14: JF,
                         P15: JF,
                         P16: JF,
                         T <: Product](
      construct: (P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16) => T,
      fieldName1: String,
      fieldName2: String,
      fieldName3: String,
      fieldName4: String,
      fieldName5: String,
      fieldName6: String,
      fieldName7: String,
      fieldName8: String,
      fieldName9: String,
      fieldName10: String,
      fieldName11: String,
      fieldName12: String,
      fieldName13: String,
      fieldName14: String,
      fieldName15: String,
      fieldName16: String
  ): RootJsonFormat[T] = new RootJsonFormat[T] {
    def write(p: T) = {
      val fields = new collection.mutable.ListBuffer[(String, JsValue)]
      fields.sizeHint(16 * 17)
      fields ++= productElement2Field[P1](fieldName1, p, 0)
      fields ++= productElement2Field[P2](fieldName2, p, 1)
      fields ++= productElement2Field[P3](fieldName3, p, 2)
      fields ++= productElement2Field[P4](fieldName4, p, 3)
      fields ++= productElement2Field[P5](fieldName5, p, 4)
      fields ++= productElement2Field[P6](fieldName6, p, 5)
      fields ++= productElement2Field[P7](fieldName7, p, 6)
      fields ++= productElement2Field[P8](fieldName8, p, 7)
      fields ++= productElement2Field[P9](fieldName9, p, 8)
      fields ++= productElement2Field[P10](fieldName10, p, 9)
      fields ++= productElement2Field[P11](fieldName11, p, 10)
      fields ++= productElement2Field[P12](fieldName12, p, 11)
      fields ++= productElement2Field[P13](fieldName13, p, 12)
      fields ++= productElement2Field[P14](fieldName14, p, 13)
      fields ++= productElement2Field[P15](fieldName15, p, 14)
      fields ++= productElement2Field[P16](fieldName16, p, 15)
      JsObject(fields.toSeq: _*)
    }
    def read(value: JsValue) = {
      val f = value.asJsObject.fields("f")
      val p1V = fromBigQueryField[P1](f, fieldName1, 0)
      val p2V = fromBigQueryField[P2](f, fieldName2, 1)
      val p3V = fromBigQueryField[P3](f, fieldName3, 2)
      val p4V = fromBigQueryField[P4](f, fieldName4, 3)
      val p5V = fromBigQueryField[P5](f, fieldName5, 4)
      val p6V = fromBigQueryField[P6](f, fieldName6, 5)
      val p7V = fromBigQueryField[P7](f, fieldName7, 6)
      val p8V = fromBigQueryField[P8](f, fieldName8, 7)
      val p9V = fromBigQueryField[P9](f, fieldName9, 8)
      val p10V = fromBigQueryField[P10](f, fieldName10, 9)
      val p11V = fromBigQueryField[P11](f, fieldName11, 10)
      val p12V = fromBigQueryField[P12](f, fieldName12, 11)
      val p13V = fromBigQueryField[P13](f, fieldName13, 12)
      val p14V = fromBigQueryField[P14](f, fieldName14, 13)
      val p15V = fromBigQueryField[P15](f, fieldName15, 14)
      val p16V = fromBigQueryField[P16](f, fieldName16, 15)
      construct(p1V, p2V, p3V, p4V, p5V, p6V, p7V, p8V, p9V, p10V, p11V, p12V, p13V, p14V, p15V, p16V)
    }
  }

  // Case classes with 17 parameters

  def bigQueryJsonFormat17[P1: JF,
                           P2: JF,
                           P3: JF,
                           P4: JF,
                           P5: JF,
                           P6: JF,
                           P7: JF,
                           P8: JF,
                           P9: JF,
                           P10: JF,
                           P11: JF,
                           P12: JF,
                           P13: JF,
                           P14: JF,
                           P15: JF,
                           P16: JF,
                           P17: JF,
                           T <: Product: ClassTag](
      construct: (P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17) => T
  ): RootJsonFormat[T] = {
    val Array(p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17) = extractFieldNames(
      classTag[T]
    )
    bigQueryJsonFormat(construct, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17)
  }
  def bigQueryJsonFormat[P1: JF,
                         P2: JF,
                         P3: JF,
                         P4: JF,
                         P5: JF,
                         P6: JF,
                         P7: JF,
                         P8: JF,
                         P9: JF,
                         P10: JF,
                         P11: JF,
                         P12: JF,
                         P13: JF,
                         P14: JF,
                         P15: JF,
                         P16: JF,
                         P17: JF,
                         T <: Product](
      construct: (P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17) => T,
      fieldName1: String,
      fieldName2: String,
      fieldName3: String,
      fieldName4: String,
      fieldName5: String,
      fieldName6: String,
      fieldName7: String,
      fieldName8: String,
      fieldName9: String,
      fieldName10: String,
      fieldName11: String,
      fieldName12: String,
      fieldName13: String,
      fieldName14: String,
      fieldName15: String,
      fieldName16: String,
      fieldName17: String
  ): RootJsonFormat[T] = new RootJsonFormat[T] {
    def write(p: T) = {
      val fields = new collection.mutable.ListBuffer[(String, JsValue)]
      fields.sizeHint(17 * 18)
      fields ++= productElement2Field[P1](fieldName1, p, 0)
      fields ++= productElement2Field[P2](fieldName2, p, 1)
      fields ++= productElement2Field[P3](fieldName3, p, 2)
      fields ++= productElement2Field[P4](fieldName4, p, 3)
      fields ++= productElement2Field[P5](fieldName5, p, 4)
      fields ++= productElement2Field[P6](fieldName6, p, 5)
      fields ++= productElement2Field[P7](fieldName7, p, 6)
      fields ++= productElement2Field[P8](fieldName8, p, 7)
      fields ++= productElement2Field[P9](fieldName9, p, 8)
      fields ++= productElement2Field[P10](fieldName10, p, 9)
      fields ++= productElement2Field[P11](fieldName11, p, 10)
      fields ++= productElement2Field[P12](fieldName12, p, 11)
      fields ++= productElement2Field[P13](fieldName13, p, 12)
      fields ++= productElement2Field[P14](fieldName14, p, 13)
      fields ++= productElement2Field[P15](fieldName15, p, 14)
      fields ++= productElement2Field[P16](fieldName16, p, 15)
      fields ++= productElement2Field[P17](fieldName17, p, 16)
      JsObject(fields.toSeq: _*)
    }
    def read(value: JsValue) = {
      val f = value.asJsObject.fields("f")
      val p1V = fromBigQueryField[P1](f, fieldName1, 0)
      val p2V = fromBigQueryField[P2](f, fieldName2, 1)
      val p3V = fromBigQueryField[P3](f, fieldName3, 2)
      val p4V = fromBigQueryField[P4](f, fieldName4, 3)
      val p5V = fromBigQueryField[P5](f, fieldName5, 4)
      val p6V = fromBigQueryField[P6](f, fieldName6, 5)
      val p7V = fromBigQueryField[P7](f, fieldName7, 6)
      val p8V = fromBigQueryField[P8](f, fieldName8, 7)
      val p9V = fromBigQueryField[P9](f, fieldName9, 8)
      val p10V = fromBigQueryField[P10](f, fieldName10, 9)
      val p11V = fromBigQueryField[P11](f, fieldName11, 10)
      val p12V = fromBigQueryField[P12](f, fieldName12, 11)
      val p13V = fromBigQueryField[P13](f, fieldName13, 12)
      val p14V = fromBigQueryField[P14](f, fieldName14, 13)
      val p15V = fromBigQueryField[P15](f, fieldName15, 14)
      val p16V = fromBigQueryField[P16](f, fieldName16, 15)
      val p17V = fromBigQueryField[P17](f, fieldName17, 16)
      construct(p1V, p2V, p3V, p4V, p5V, p6V, p7V, p8V, p9V, p10V, p11V, p12V, p13V, p14V, p15V, p16V, p17V)
    }
  }

  // Case classes with 18 parameters

  def bigQueryJsonFormat18[P1: JF,
                           P2: JF,
                           P3: JF,
                           P4: JF,
                           P5: JF,
                           P6: JF,
                           P7: JF,
                           P8: JF,
                           P9: JF,
                           P10: JF,
                           P11: JF,
                           P12: JF,
                           P13: JF,
                           P14: JF,
                           P15: JF,
                           P16: JF,
                           P17: JF,
                           P18: JF,
                           T <: Product: ClassTag](
      construct: (P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18) => T
  ): RootJsonFormat[T] = {
    val Array(p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18) = extractFieldNames(
      classTag[T]
    )
    bigQueryJsonFormat(construct, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18)
  }
  def bigQueryJsonFormat[P1: JF,
                         P2: JF,
                         P3: JF,
                         P4: JF,
                         P5: JF,
                         P6: JF,
                         P7: JF,
                         P8: JF,
                         P9: JF,
                         P10: JF,
                         P11: JF,
                         P12: JF,
                         P13: JF,
                         P14: JF,
                         P15: JF,
                         P16: JF,
                         P17: JF,
                         P18: JF,
                         T <: Product](
      construct: (P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18) => T,
      fieldName1: String,
      fieldName2: String,
      fieldName3: String,
      fieldName4: String,
      fieldName5: String,
      fieldName6: String,
      fieldName7: String,
      fieldName8: String,
      fieldName9: String,
      fieldName10: String,
      fieldName11: String,
      fieldName12: String,
      fieldName13: String,
      fieldName14: String,
      fieldName15: String,
      fieldName16: String,
      fieldName17: String,
      fieldName18: String
  ): RootJsonFormat[T] = new RootJsonFormat[T] {
    def write(p: T) = {
      val fields = new collection.mutable.ListBuffer[(String, JsValue)]
      fields.sizeHint(18 * 19)
      fields ++= productElement2Field[P1](fieldName1, p, 0)
      fields ++= productElement2Field[P2](fieldName2, p, 1)
      fields ++= productElement2Field[P3](fieldName3, p, 2)
      fields ++= productElement2Field[P4](fieldName4, p, 3)
      fields ++= productElement2Field[P5](fieldName5, p, 4)
      fields ++= productElement2Field[P6](fieldName6, p, 5)
      fields ++= productElement2Field[P7](fieldName7, p, 6)
      fields ++= productElement2Field[P8](fieldName8, p, 7)
      fields ++= productElement2Field[P9](fieldName9, p, 8)
      fields ++= productElement2Field[P10](fieldName10, p, 9)
      fields ++= productElement2Field[P11](fieldName11, p, 10)
      fields ++= productElement2Field[P12](fieldName12, p, 11)
      fields ++= productElement2Field[P13](fieldName13, p, 12)
      fields ++= productElement2Field[P14](fieldName14, p, 13)
      fields ++= productElement2Field[P15](fieldName15, p, 14)
      fields ++= productElement2Field[P16](fieldName16, p, 15)
      fields ++= productElement2Field[P17](fieldName17, p, 16)
      fields ++= productElement2Field[P18](fieldName18, p, 17)
      JsObject(fields.toSeq: _*)
    }
    def read(value: JsValue) = {
      val f = value.asJsObject.fields("f")
      val p1V = fromBigQueryField[P1](f, fieldName1, 0)
      val p2V = fromBigQueryField[P2](f, fieldName2, 1)
      val p3V = fromBigQueryField[P3](f, fieldName3, 2)
      val p4V = fromBigQueryField[P4](f, fieldName4, 3)
      val p5V = fromBigQueryField[P5](f, fieldName5, 4)
      val p6V = fromBigQueryField[P6](f, fieldName6, 5)
      val p7V = fromBigQueryField[P7](f, fieldName7, 6)
      val p8V = fromBigQueryField[P8](f, fieldName8, 7)
      val p9V = fromBigQueryField[P9](f, fieldName9, 8)
      val p10V = fromBigQueryField[P10](f, fieldName10, 9)
      val p11V = fromBigQueryField[P11](f, fieldName11, 10)
      val p12V = fromBigQueryField[P12](f, fieldName12, 11)
      val p13V = fromBigQueryField[P13](f, fieldName13, 12)
      val p14V = fromBigQueryField[P14](f, fieldName14, 13)
      val p15V = fromBigQueryField[P15](f, fieldName15, 14)
      val p16V = fromBigQueryField[P16](f, fieldName16, 15)
      val p17V = fromBigQueryField[P17](f, fieldName17, 16)
      val p18V = fromBigQueryField[P18](f, fieldName18, 17)
      construct(p1V, p2V, p3V, p4V, p5V, p6V, p7V, p8V, p9V, p10V, p11V, p12V, p13V, p14V, p15V, p16V, p17V, p18V)
    }
  }

  // Case classes with 19 parameters

  def bigQueryJsonFormat19[P1: JF,
                           P2: JF,
                           P3: JF,
                           P4: JF,
                           P5: JF,
                           P6: JF,
                           P7: JF,
                           P8: JF,
                           P9: JF,
                           P10: JF,
                           P11: JF,
                           P12: JF,
                           P13: JF,
                           P14: JF,
                           P15: JF,
                           P16: JF,
                           P17: JF,
                           P18: JF,
                           P19: JF,
                           T <: Product: ClassTag](
      construct: (P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, P19) => T
  ): RootJsonFormat[T] = {
    val Array(p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18, p19) = extractFieldNames(
      classTag[T]
    )
    bigQueryJsonFormat(construct, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18, p19)
  }
  def bigQueryJsonFormat[P1: JF,
                         P2: JF,
                         P3: JF,
                         P4: JF,
                         P5: JF,
                         P6: JF,
                         P7: JF,
                         P8: JF,
                         P9: JF,
                         P10: JF,
                         P11: JF,
                         P12: JF,
                         P13: JF,
                         P14: JF,
                         P15: JF,
                         P16: JF,
                         P17: JF,
                         P18: JF,
                         P19: JF,
                         T <: Product](
      construct: (P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, P19) => T,
      fieldName1: String,
      fieldName2: String,
      fieldName3: String,
      fieldName4: String,
      fieldName5: String,
      fieldName6: String,
      fieldName7: String,
      fieldName8: String,
      fieldName9: String,
      fieldName10: String,
      fieldName11: String,
      fieldName12: String,
      fieldName13: String,
      fieldName14: String,
      fieldName15: String,
      fieldName16: String,
      fieldName17: String,
      fieldName18: String,
      fieldName19: String
  ): RootJsonFormat[T] = new RootJsonFormat[T] {
    def write(p: T) = {
      val fields = new collection.mutable.ListBuffer[(String, JsValue)]
      fields.sizeHint(19 * 20)
      fields ++= productElement2Field[P1](fieldName1, p, 0)
      fields ++= productElement2Field[P2](fieldName2, p, 1)
      fields ++= productElement2Field[P3](fieldName3, p, 2)
      fields ++= productElement2Field[P4](fieldName4, p, 3)
      fields ++= productElement2Field[P5](fieldName5, p, 4)
      fields ++= productElement2Field[P6](fieldName6, p, 5)
      fields ++= productElement2Field[P7](fieldName7, p, 6)
      fields ++= productElement2Field[P8](fieldName8, p, 7)
      fields ++= productElement2Field[P9](fieldName9, p, 8)
      fields ++= productElement2Field[P10](fieldName10, p, 9)
      fields ++= productElement2Field[P11](fieldName11, p, 10)
      fields ++= productElement2Field[P12](fieldName12, p, 11)
      fields ++= productElement2Field[P13](fieldName13, p, 12)
      fields ++= productElement2Field[P14](fieldName14, p, 13)
      fields ++= productElement2Field[P15](fieldName15, p, 14)
      fields ++= productElement2Field[P16](fieldName16, p, 15)
      fields ++= productElement2Field[P17](fieldName17, p, 16)
      fields ++= productElement2Field[P18](fieldName18, p, 17)
      fields ++= productElement2Field[P19](fieldName19, p, 18)
      JsObject(fields.toSeq: _*)
    }
    def read(value: JsValue) = {
      val f = value.asJsObject.fields("f")
      val p1V = fromBigQueryField[P1](f, fieldName1, 0)
      val p2V = fromBigQueryField[P2](f, fieldName2, 1)
      val p3V = fromBigQueryField[P3](f, fieldName3, 2)
      val p4V = fromBigQueryField[P4](f, fieldName4, 3)
      val p5V = fromBigQueryField[P5](f, fieldName5, 4)
      val p6V = fromBigQueryField[P6](f, fieldName6, 5)
      val p7V = fromBigQueryField[P7](f, fieldName7, 6)
      val p8V = fromBigQueryField[P8](f, fieldName8, 7)
      val p9V = fromBigQueryField[P9](f, fieldName9, 8)
      val p10V = fromBigQueryField[P10](f, fieldName10, 9)
      val p11V = fromBigQueryField[P11](f, fieldName11, 10)
      val p12V = fromBigQueryField[P12](f, fieldName12, 11)
      val p13V = fromBigQueryField[P13](f, fieldName13, 12)
      val p14V = fromBigQueryField[P14](f, fieldName14, 13)
      val p15V = fromBigQueryField[P15](f, fieldName15, 14)
      val p16V = fromBigQueryField[P16](f, fieldName16, 15)
      val p17V = fromBigQueryField[P17](f, fieldName17, 16)
      val p18V = fromBigQueryField[P18](f, fieldName18, 17)
      val p19V = fromBigQueryField[P19](f, fieldName19, 18)
      construct(p1V, p2V, p3V, p4V, p5V, p6V, p7V, p8V, p9V, p10V, p11V, p12V, p13V, p14V, p15V, p16V, p17V, p18V, p19V)
    }
  }

  // Case classes with 20 parameters

  def bigQueryJsonFormat20[P1: JF,
                           P2: JF,
                           P3: JF,
                           P4: JF,
                           P5: JF,
                           P6: JF,
                           P7: JF,
                           P8: JF,
                           P9: JF,
                           P10: JF,
                           P11: JF,
                           P12: JF,
                           P13: JF,
                           P14: JF,
                           P15: JF,
                           P16: JF,
                           P17: JF,
                           P18: JF,
                           P19: JF,
                           P20: JF,
                           T <: Product: ClassTag](
      construct: (P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, P19, P20) => T
  ): RootJsonFormat[T] = {
    val Array(p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18, p19, p20) =
      extractFieldNames(classTag[T])
    bigQueryJsonFormat(construct,
                       p1,
                       p2,
                       p3,
                       p4,
                       p5,
                       p6,
                       p7,
                       p8,
                       p9,
                       p10,
                       p11,
                       p12,
                       p13,
                       p14,
                       p15,
                       p16,
                       p17,
                       p18,
                       p19,
                       p20)
  }
  def bigQueryJsonFormat[P1: JF,
                         P2: JF,
                         P3: JF,
                         P4: JF,
                         P5: JF,
                         P6: JF,
                         P7: JF,
                         P8: JF,
                         P9: JF,
                         P10: JF,
                         P11: JF,
                         P12: JF,
                         P13: JF,
                         P14: JF,
                         P15: JF,
                         P16: JF,
                         P17: JF,
                         P18: JF,
                         P19: JF,
                         P20: JF,
                         T <: Product](
      construct: (P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, P19, P20) => T,
      fieldName1: String,
      fieldName2: String,
      fieldName3: String,
      fieldName4: String,
      fieldName5: String,
      fieldName6: String,
      fieldName7: String,
      fieldName8: String,
      fieldName9: String,
      fieldName10: String,
      fieldName11: String,
      fieldName12: String,
      fieldName13: String,
      fieldName14: String,
      fieldName15: String,
      fieldName16: String,
      fieldName17: String,
      fieldName18: String,
      fieldName19: String,
      fieldName20: String
  ): RootJsonFormat[T] = new RootJsonFormat[T] {
    def write(p: T) = {
      val fields = new collection.mutable.ListBuffer[(String, JsValue)]
      fields.sizeHint(20 * 21)
      fields ++= productElement2Field[P1](fieldName1, p, 0)
      fields ++= productElement2Field[P2](fieldName2, p, 1)
      fields ++= productElement2Field[P3](fieldName3, p, 2)
      fields ++= productElement2Field[P4](fieldName4, p, 3)
      fields ++= productElement2Field[P5](fieldName5, p, 4)
      fields ++= productElement2Field[P6](fieldName6, p, 5)
      fields ++= productElement2Field[P7](fieldName7, p, 6)
      fields ++= productElement2Field[P8](fieldName8, p, 7)
      fields ++= productElement2Field[P9](fieldName9, p, 8)
      fields ++= productElement2Field[P10](fieldName10, p, 9)
      fields ++= productElement2Field[P11](fieldName11, p, 10)
      fields ++= productElement2Field[P12](fieldName12, p, 11)
      fields ++= productElement2Field[P13](fieldName13, p, 12)
      fields ++= productElement2Field[P14](fieldName14, p, 13)
      fields ++= productElement2Field[P15](fieldName15, p, 14)
      fields ++= productElement2Field[P16](fieldName16, p, 15)
      fields ++= productElement2Field[P17](fieldName17, p, 16)
      fields ++= productElement2Field[P18](fieldName18, p, 17)
      fields ++= productElement2Field[P19](fieldName19, p, 18)
      fields ++= productElement2Field[P20](fieldName20, p, 19)
      JsObject(fields.toSeq: _*)
    }
    def read(value: JsValue) = {
      val f = value.asJsObject.fields("f")
      val p1V = fromBigQueryField[P1](f, fieldName1, 0)
      val p2V = fromBigQueryField[P2](f, fieldName2, 1)
      val p3V = fromBigQueryField[P3](f, fieldName3, 2)
      val p4V = fromBigQueryField[P4](f, fieldName4, 3)
      val p5V = fromBigQueryField[P5](f, fieldName5, 4)
      val p6V = fromBigQueryField[P6](f, fieldName6, 5)
      val p7V = fromBigQueryField[P7](f, fieldName7, 6)
      val p8V = fromBigQueryField[P8](f, fieldName8, 7)
      val p9V = fromBigQueryField[P9](f, fieldName9, 8)
      val p10V = fromBigQueryField[P10](f, fieldName10, 9)
      val p11V = fromBigQueryField[P11](f, fieldName11, 10)
      val p12V = fromBigQueryField[P12](f, fieldName12, 11)
      val p13V = fromBigQueryField[P13](f, fieldName13, 12)
      val p14V = fromBigQueryField[P14](f, fieldName14, 13)
      val p15V = fromBigQueryField[P15](f, fieldName15, 14)
      val p16V = fromBigQueryField[P16](f, fieldName16, 15)
      val p17V = fromBigQueryField[P17](f, fieldName17, 16)
      val p18V = fromBigQueryField[P18](f, fieldName18, 17)
      val p19V = fromBigQueryField[P19](f, fieldName19, 18)
      val p20V = fromBigQueryField[P20](f, fieldName20, 19)
      construct(p1V,
                p2V,
                p3V,
                p4V,
                p5V,
                p6V,
                p7V,
                p8V,
                p9V,
                p10V,
                p11V,
                p12V,
                p13V,
                p14V,
                p15V,
                p16V,
                p17V,
                p18V,
                p19V,
                p20V)
    }
  }

  // Case classes with 21 parameters

  def bigQueryJsonFormat21[P1: JF,
                           P2: JF,
                           P3: JF,
                           P4: JF,
                           P5: JF,
                           P6: JF,
                           P7: JF,
                           P8: JF,
                           P9: JF,
                           P10: JF,
                           P11: JF,
                           P12: JF,
                           P13: JF,
                           P14: JF,
                           P15: JF,
                           P16: JF,
                           P17: JF,
                           P18: JF,
                           P19: JF,
                           P20: JF,
                           P21: JF,
                           T <: Product: ClassTag](
      construct: (P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, P19, P20, P21) => T
  ): RootJsonFormat[T] = {
    val Array(p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18, p19, p20, p21) =
      extractFieldNames(classTag[T])
    bigQueryJsonFormat(construct,
                       p1,
                       p2,
                       p3,
                       p4,
                       p5,
                       p6,
                       p7,
                       p8,
                       p9,
                       p10,
                       p11,
                       p12,
                       p13,
                       p14,
                       p15,
                       p16,
                       p17,
                       p18,
                       p19,
                       p20,
                       p21)
  }
  def bigQueryJsonFormat[P1: JF,
                         P2: JF,
                         P3: JF,
                         P4: JF,
                         P5: JF,
                         P6: JF,
                         P7: JF,
                         P8: JF,
                         P9: JF,
                         P10: JF,
                         P11: JF,
                         P12: JF,
                         P13: JF,
                         P14: JF,
                         P15: JF,
                         P16: JF,
                         P17: JF,
                         P18: JF,
                         P19: JF,
                         P20: JF,
                         P21: JF,
                         T <: Product](
      construct: (P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, P19, P20, P21) => T,
      fieldName1: String,
      fieldName2: String,
      fieldName3: String,
      fieldName4: String,
      fieldName5: String,
      fieldName6: String,
      fieldName7: String,
      fieldName8: String,
      fieldName9: String,
      fieldName10: String,
      fieldName11: String,
      fieldName12: String,
      fieldName13: String,
      fieldName14: String,
      fieldName15: String,
      fieldName16: String,
      fieldName17: String,
      fieldName18: String,
      fieldName19: String,
      fieldName20: String,
      fieldName21: String
  ): RootJsonFormat[T] = new RootJsonFormat[T] {
    def write(p: T) = {
      val fields = new collection.mutable.ListBuffer[(String, JsValue)]
      fields.sizeHint(21 * 22)
      fields ++= productElement2Field[P1](fieldName1, p, 0)
      fields ++= productElement2Field[P2](fieldName2, p, 1)
      fields ++= productElement2Field[P3](fieldName3, p, 2)
      fields ++= productElement2Field[P4](fieldName4, p, 3)
      fields ++= productElement2Field[P5](fieldName5, p, 4)
      fields ++= productElement2Field[P6](fieldName6, p, 5)
      fields ++= productElement2Field[P7](fieldName7, p, 6)
      fields ++= productElement2Field[P8](fieldName8, p, 7)
      fields ++= productElement2Field[P9](fieldName9, p, 8)
      fields ++= productElement2Field[P10](fieldName10, p, 9)
      fields ++= productElement2Field[P11](fieldName11, p, 10)
      fields ++= productElement2Field[P12](fieldName12, p, 11)
      fields ++= productElement2Field[P13](fieldName13, p, 12)
      fields ++= productElement2Field[P14](fieldName14, p, 13)
      fields ++= productElement2Field[P15](fieldName15, p, 14)
      fields ++= productElement2Field[P16](fieldName16, p, 15)
      fields ++= productElement2Field[P17](fieldName17, p, 16)
      fields ++= productElement2Field[P18](fieldName18, p, 17)
      fields ++= productElement2Field[P19](fieldName19, p, 18)
      fields ++= productElement2Field[P20](fieldName20, p, 19)
      fields ++= productElement2Field[P21](fieldName21, p, 20)
      JsObject(fields.toSeq: _*)
    }
    def read(value: JsValue) = {
      val f = value.asJsObject.fields("f")
      val p1V = fromBigQueryField[P1](f, fieldName1, 0)
      val p2V = fromBigQueryField[P2](f, fieldName2, 1)
      val p3V = fromBigQueryField[P3](f, fieldName3, 2)
      val p4V = fromBigQueryField[P4](f, fieldName4, 3)
      val p5V = fromBigQueryField[P5](f, fieldName5, 4)
      val p6V = fromBigQueryField[P6](f, fieldName6, 5)
      val p7V = fromBigQueryField[P7](f, fieldName7, 6)
      val p8V = fromBigQueryField[P8](f, fieldName8, 7)
      val p9V = fromBigQueryField[P9](f, fieldName9, 8)
      val p10V = fromBigQueryField[P10](f, fieldName10, 9)
      val p11V = fromBigQueryField[P11](f, fieldName11, 10)
      val p12V = fromBigQueryField[P12](f, fieldName12, 11)
      val p13V = fromBigQueryField[P13](f, fieldName13, 12)
      val p14V = fromBigQueryField[P14](f, fieldName14, 13)
      val p15V = fromBigQueryField[P15](f, fieldName15, 14)
      val p16V = fromBigQueryField[P16](f, fieldName16, 15)
      val p17V = fromBigQueryField[P17](f, fieldName17, 16)
      val p18V = fromBigQueryField[P18](f, fieldName18, 17)
      val p19V = fromBigQueryField[P19](f, fieldName19, 18)
      val p20V = fromBigQueryField[P20](f, fieldName20, 19)
      val p21V = fromBigQueryField[P21](f, fieldName21, 20)
      construct(p1V,
                p2V,
                p3V,
                p4V,
                p5V,
                p6V,
                p7V,
                p8V,
                p9V,
                p10V,
                p11V,
                p12V,
                p13V,
                p14V,
                p15V,
                p16V,
                p17V,
                p18V,
                p19V,
                p20V,
                p21V)
    }
  }

  // Case classes with 22 parameters

  def bigQueryJsonFormat22[P1: JF,
                           P2: JF,
                           P3: JF,
                           P4: JF,
                           P5: JF,
                           P6: JF,
                           P7: JF,
                           P8: JF,
                           P9: JF,
                           P10: JF,
                           P11: JF,
                           P12: JF,
                           P13: JF,
                           P14: JF,
                           P15: JF,
                           P16: JF,
                           P17: JF,
                           P18: JF,
                           P19: JF,
                           P20: JF,
                           P21: JF,
                           P22: JF,
                           T <: Product: ClassTag](
      construct: (P1,
                  P2,
                  P3,
                  P4,
                  P5,
                  P6,
                  P7,
                  P8,
                  P9,
                  P10,
                  P11,
                  P12,
                  P13,
                  P14,
                  P15,
                  P16,
                  P17,
                  P18,
                  P19,
                  P20,
                  P21,
                  P22) => T
  ): RootJsonFormat[T] = {
    val Array(p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18, p19, p20, p21, p22) =
      extractFieldNames(classTag[T])
    bigQueryJsonFormat(construct,
                       p1,
                       p2,
                       p3,
                       p4,
                       p5,
                       p6,
                       p7,
                       p8,
                       p9,
                       p10,
                       p11,
                       p12,
                       p13,
                       p14,
                       p15,
                       p16,
                       p17,
                       p18,
                       p19,
                       p20,
                       p21,
                       p22)
  }
  def bigQueryJsonFormat[P1: JF,
                         P2: JF,
                         P3: JF,
                         P4: JF,
                         P5: JF,
                         P6: JF,
                         P7: JF,
                         P8: JF,
                         P9: JF,
                         P10: JF,
                         P11: JF,
                         P12: JF,
                         P13: JF,
                         P14: JF,
                         P15: JF,
                         P16: JF,
                         P17: JF,
                         P18: JF,
                         P19: JF,
                         P20: JF,
                         P21: JF,
                         P22: JF,
                         T <: Product](construct: (P1,
                                                   P2,
                                                   P3,
                                                   P4,
                                                   P5,
                                                   P6,
                                                   P7,
                                                   P8,
                                                   P9,
                                                   P10,
                                                   P11,
                                                   P12,
                                                   P13,
                                                   P14,
                                                   P15,
                                                   P16,
                                                   P17,
                                                   P18,
                                                   P19,
                                                   P20,
                                                   P21,
                                                   P22) => T,
                                       fieldName1: String,
                                       fieldName2: String,
                                       fieldName3: String,
                                       fieldName4: String,
                                       fieldName5: String,
                                       fieldName6: String,
                                       fieldName7: String,
                                       fieldName8: String,
                                       fieldName9: String,
                                       fieldName10: String,
                                       fieldName11: String,
                                       fieldName12: String,
                                       fieldName13: String,
                                       fieldName14: String,
                                       fieldName15: String,
                                       fieldName16: String,
                                       fieldName17: String,
                                       fieldName18: String,
                                       fieldName19: String,
                                       fieldName20: String,
                                       fieldName21: String,
                                       fieldName22: String): RootJsonFormat[T] = new RootJsonFormat[T] {
    def write(p: T) = {
      val fields = new collection.mutable.ListBuffer[(String, JsValue)]
      fields.sizeHint(22 * 23)
      fields ++= productElement2Field[P1](fieldName1, p, 0)
      fields ++= productElement2Field[P2](fieldName2, p, 1)
      fields ++= productElement2Field[P3](fieldName3, p, 2)
      fields ++= productElement2Field[P4](fieldName4, p, 3)
      fields ++= productElement2Field[P5](fieldName5, p, 4)
      fields ++= productElement2Field[P6](fieldName6, p, 5)
      fields ++= productElement2Field[P7](fieldName7, p, 6)
      fields ++= productElement2Field[P8](fieldName8, p, 7)
      fields ++= productElement2Field[P9](fieldName9, p, 8)
      fields ++= productElement2Field[P10](fieldName10, p, 9)
      fields ++= productElement2Field[P11](fieldName11, p, 10)
      fields ++= productElement2Field[P12](fieldName12, p, 11)
      fields ++= productElement2Field[P13](fieldName13, p, 12)
      fields ++= productElement2Field[P14](fieldName14, p, 13)
      fields ++= productElement2Field[P15](fieldName15, p, 14)
      fields ++= productElement2Field[P16](fieldName16, p, 15)
      fields ++= productElement2Field[P17](fieldName17, p, 16)
      fields ++= productElement2Field[P18](fieldName18, p, 17)
      fields ++= productElement2Field[P19](fieldName19, p, 18)
      fields ++= productElement2Field[P20](fieldName20, p, 19)
      fields ++= productElement2Field[P21](fieldName21, p, 20)
      fields ++= productElement2Field[P22](fieldName22, p, 21)
      JsObject(fields.toSeq: _*)
    }
    def read(value: JsValue) = {
      val f = value.asJsObject.fields("f")
      val p1V = fromBigQueryField[P1](f, fieldName1, 0)
      val p2V = fromBigQueryField[P2](f, fieldName2, 1)
      val p3V = fromBigQueryField[P3](f, fieldName3, 2)
      val p4V = fromBigQueryField[P4](f, fieldName4, 3)
      val p5V = fromBigQueryField[P5](f, fieldName5, 4)
      val p6V = fromBigQueryField[P6](f, fieldName6, 5)
      val p7V = fromBigQueryField[P7](f, fieldName7, 6)
      val p8V = fromBigQueryField[P8](f, fieldName8, 7)
      val p9V = fromBigQueryField[P9](f, fieldName9, 8)
      val p10V = fromBigQueryField[P10](f, fieldName10, 9)
      val p11V = fromBigQueryField[P11](f, fieldName11, 10)
      val p12V = fromBigQueryField[P12](f, fieldName12, 11)
      val p13V = fromBigQueryField[P13](f, fieldName13, 12)
      val p14V = fromBigQueryField[P14](f, fieldName14, 13)
      val p15V = fromBigQueryField[P15](f, fieldName15, 14)
      val p16V = fromBigQueryField[P16](f, fieldName16, 15)
      val p17V = fromBigQueryField[P17](f, fieldName17, 16)
      val p18V = fromBigQueryField[P18](f, fieldName18, 17)
      val p19V = fromBigQueryField[P19](f, fieldName19, 18)
      val p20V = fromBigQueryField[P20](f, fieldName20, 19)
      val p21V = fromBigQueryField[P21](f, fieldName21, 20)
      val p22V = fromBigQueryField[P22](f, fieldName22, 21)
      construct(p1V,
                p2V,
                p3V,
                p4V,
                p5V,
                p6V,
                p7V,
                p8V,
                p9V,
                p10V,
                p11V,
                p12V,
                p13V,
                p14V,
                p15V,
                p16V,
                p17V,
                p18V,
                p19V,
                p20V,
                p21V,
                p22V)
    }
  }

}
