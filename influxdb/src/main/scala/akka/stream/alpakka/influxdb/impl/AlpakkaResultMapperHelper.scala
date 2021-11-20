/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.influxdb.impl

import java.lang.reflect.Field
import java.time.Instant
import java.time.format.DateTimeFormatterBuilder
import java.time.temporal.ChronoField
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}
import org.influxdb.annotation.{Column, Measurement}
import org.influxdb.dto.QueryResult

import java.util.concurrent.TimeUnit
import akka.annotation.InternalApi
import org.influxdb.InfluxDBMapperException
import org.influxdb.dto.Point

import scala.annotation.nowarn
import scala.jdk.CollectionConverters._

/**
 * Internal API.
 */
@InternalApi
private[impl] class AlpakkaResultMapperHelper {

  val CLASS_FIELD_CACHE: ConcurrentHashMap[String, ConcurrentMap[String, Field]] = new ConcurrentHashMap();

  private val FRACTION_MIN_WIDTH = 0
  private val FRACTION_MAX_WIDTH = 9
  private val ADD_DECIMAL_POINT = true

  private val RFC3339_FORMATTER = new DateTimeFormatterBuilder()
    .appendPattern("yyyy-MM-dd'T'HH:mm:ss")
    .appendFraction(ChronoField.NANO_OF_SECOND, FRACTION_MIN_WIDTH, FRACTION_MAX_WIDTH, ADD_DECIMAL_POINT)
    .appendZoneOrOffsetId
    .toFormatter

  private[impl] def databaseName(point: Class[_]): String =
    point.getAnnotation(classOf[Measurement]).database();

  private[impl] def retentionPolicy(point: Class[_]): String =
    point.getAnnotation(classOf[Measurement]).retentionPolicy();

  private[impl] def convertModelToPoint[T](model: T): Point = {
    throwExceptionIfMissingAnnotation(model.getClass)
    cacheClassFields(model.getClass)

    val colNameAndFieldMap: ConcurrentMap[String, Field] = CLASS_FIELD_CACHE.get(model.getClass.getName)

    try {
      val modelType = model.getClass();
      val measurement = measurementName(modelType);
      val timeUnit: TimeUnit = this.timeUnit(modelType);
      val time = timeUnit.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
      val pointBuilder: Point.Builder = Point.measurement(measurement).time(time, timeUnit);

      for (key <- colNameAndFieldMap.keySet().asScala) {
        val field = colNameAndFieldMap.get(key)
        val column = field.getAnnotation(classOf[Column])
        val columnName: String = column.name()
        val fieldType: Class[_] = field.getType()

        val isAccessible = field.isAccessible(): @nowarn("msg=deprecated")
        if (!isAccessible) {
          field.setAccessible(true);
        }

        val value = field.get(model);

        if (column.tag()) {
          pointBuilder.tag(columnName, value.toString());
        } else if ("time".equals(columnName)) {
          if (value != null) {
            setTime(pointBuilder, fieldType, timeUnit, value);
          }
        } else {
          setField(pointBuilder, fieldType, columnName, value);
        }
      }

      pointBuilder.build();
    } catch {
      case e: IllegalArgumentException => throw new InfluxDBMapperException(e);
    }
  }

  private[impl] def cacheClassFields(clazz: Class[_]) =
    if (!CLASS_FIELD_CACHE.containsKey(clazz.getName)) {
      val initialMap: ConcurrentMap[String, Field] = new ConcurrentHashMap()
      var influxColumnAndFieldMap = CLASS_FIELD_CACHE.putIfAbsent(clazz.getName, initialMap)

      if (influxColumnAndFieldMap == null) {
        influxColumnAndFieldMap = initialMap;
      }

      var c = clazz;

      while (c != null) {
        for (field <- c.getDeclaredFields()) {
          val colAnnotation = field.getAnnotation(classOf[Column]);
          if (colAnnotation != null) {
            influxColumnAndFieldMap.put(colAnnotation.name(), field);
          }
        }
        c = c.getSuperclass();
      }
    }

  private[impl] def parseSeriesAs[T](clazz: Class[T], series: QueryResult.Series, precision: TimeUnit): List[T] = {
    cacheClassFields(clazz)
    series.getValues.asScala
      .map((v: java.util.List[AnyRef]) => parseRowAs(clazz, series.getColumns, v, precision))
      .toList
  }

  private def measurementName(point: Class[_]): String =
    point.getAnnotation(classOf[Measurement]).name();

  private def timeUnit(point: Class[_]): TimeUnit =
    point.getAnnotation(classOf[Measurement]).timeUnit()

  private def setTime(pointBuilder: Point.Builder, fieldType: Class[_], timeUnit: TimeUnit, value: Any): Unit =
    if (classOf[Instant].isAssignableFrom(fieldType)) {
      val instant = value.asInstanceOf[Instant]
      val time = timeUnit.convert(instant.toEpochMilli, TimeUnit.MILLISECONDS)
      pointBuilder.time(time, timeUnit)
    } else throw new InfluxDBMapperException("Unsupported type " + fieldType + " for time: should be of Instant type")

  private def setField(pointBuilder: Point.Builder, fieldType: Class[_], columnName: String, value: Any): Unit =
    if (classOf[java.lang.Boolean].isAssignableFrom(fieldType) || classOf[Boolean].isAssignableFrom(fieldType))
      pointBuilder.addField(columnName, value.asInstanceOf[Boolean])
    else if (classOf[java.lang.Long].isAssignableFrom(fieldType) || classOf[Long].isAssignableFrom(fieldType))
      pointBuilder.addField(columnName, value.asInstanceOf[Long])
    else if (classOf[java.lang.Double].isAssignableFrom(fieldType) || classOf[Double].isAssignableFrom(fieldType))
      pointBuilder.addField(columnName, value.asInstanceOf[Double])
    else if (classOf[java.lang.Integer].isAssignableFrom(fieldType) || classOf[Integer].isAssignableFrom(fieldType))
      pointBuilder.addField(columnName, value.asInstanceOf[Int])
    else if (classOf[String].isAssignableFrom(fieldType)) pointBuilder.addField(columnName, value.asInstanceOf[String])
    else throw new InfluxDBMapperException("Unsupported type " + fieldType + " for column " + columnName)

  private def throwExceptionIfMissingAnnotation(clazz: Class[_]): Unit =
    if (!clazz.isAnnotationPresent(classOf[Measurement]))
      throw new IllegalArgumentException(
        "Class " + clazz.getName + " is not annotated with @" + classOf[Measurement].getSimpleName
      )

  private def parseRowAs[T](clazz: Class[T],
                            columns: java.util.List[String],
                            values: java.util.List[AnyRef],
                            precision: TimeUnit): T =
    try {
      val fieldMap = CLASS_FIELD_CACHE.get(clazz.getName)

      val obj: T = clazz.getDeclaredConstructor().newInstance()
      for (i <- 0 until columns.size()) {
        val correspondingField = fieldMap.get(columns.get(i))
        if (correspondingField != null) {
          setFieldValue(obj, correspondingField, values.get(i), precision)
        }
      }
      obj
    } catch {
      case e @ (_: InstantiationException | _: IllegalAccessException) =>
        throw new InfluxDBMapperException(e)
    }

  @throws[IllegalArgumentException]
  @throws[IllegalAccessException]
  private def setFieldValue[T](obj: T, field: Field, value: Any, precision: TimeUnit): Unit = {
    if (value == null) return
    val fieldType = field.getType
    try {
      val isAccessible = field.isAccessible(): @nowarn("msg=deprecated")
      if (!isAccessible) field.setAccessible(true)
      if (fieldValueModified(fieldType, field, obj, value, precision) || fieldValueForPrimitivesModified(
            fieldType,
            field,
            obj,
            value
          ) || fieldValueForPrimitiveWrappersModified(fieldType, field, obj, value)) return
      val msg =
        s"""Class '${obj.getClass.getName}' field '${field.getName}' is from an unsupported type '${field.getType}'."""
      throw new InfluxDBMapperException(msg)
    } catch {
      case e: ClassCastException =>
        val msg =
          s"""Class '${obj.getClass.getName}' field '${field.getName}' was defined with a different field type and caused a ClassCastException.
             |The correct type is '${value.getClass.getName}' (current field value: '${value}')""".stripMargin
        throw new InfluxDBMapperException(msg)
    }
  }

  @throws[IllegalArgumentException]
  @throws[IllegalAccessException]
  private def fieldValueForPrimitivesModified[T](fieldType: Class[_], field: Field, obj: T, value: Any): Boolean =
    if (classOf[Double].isAssignableFrom(fieldType)) {
      field.setDouble(obj, value.asInstanceOf[Double].doubleValue)
      true
    } else if (classOf[Long].isAssignableFrom(fieldType)) {
      field.setLong(obj, value.asInstanceOf[Double].longValue)
      true
    } else if (classOf[Int].isAssignableFrom(fieldType)) {
      field.setInt(obj, value.asInstanceOf[Double].intValue)
      true
    } else if (classOf[Boolean].isAssignableFrom(fieldType)) {
      field.setBoolean(obj, String.valueOf(value).toBoolean)
      true
    } else {
      false
    }

  @throws[IllegalArgumentException]
  @throws[IllegalAccessException]
  private def fieldValueForPrimitiveWrappersModified[T](fieldType: Class[_],
                                                        field: Field,
                                                        obj: T,
                                                        value: Any): Boolean =
    if (classOf[java.lang.Double].isAssignableFrom(fieldType)) {
      field.set(obj, value)
      true
    } else if (classOf[java.lang.Long].isAssignableFrom(fieldType)) {
      field.set(obj, value.asInstanceOf[Double].longValue())
      true
    } else if (classOf[Integer].isAssignableFrom(fieldType)) {
      field.set(obj, value.asInstanceOf[java.lang.Integer])
      true
    } else if (classOf[java.lang.Boolean].isAssignableFrom(fieldType)) {
      field.set(obj, value.asInstanceOf[java.lang.Boolean])
      true
    } else {
      false
    }

  @throws[IllegalArgumentException]
  @throws[IllegalAccessException]
  private def fieldValueModified[T](fieldType: Class[_],
                                    field: Field,
                                    obj: T,
                                    value: Any,
                                    precision: TimeUnit): Boolean =
    if (classOf[String].isAssignableFrom(fieldType)) {
      field.set(obj, String.valueOf(value))
      true
    } else if (classOf[Instant].isAssignableFrom(fieldType)) {
      val instant: Instant = getInstant(field, value, precision)
      field.set(obj, instant)
      true
    } else {
      false
    }

  private def getInstant(field: Field, value: Any, precision: TimeUnit): Instant =
    if (value.isInstanceOf[String]) Instant.from(RFC3339_FORMATTER.parse(String.valueOf(value)))
    else if (value.isInstanceOf[java.lang.Long]) Instant.ofEpochMilli(toMillis(value.asInstanceOf[Long], precision))
    else if (value.isInstanceOf[java.lang.Double])
      Instant.ofEpochMilli(toMillis(value.asInstanceOf[java.lang.Double].longValue, precision))
    else if (value.isInstanceOf[java.lang.Integer])
      Instant.ofEpochMilli(toMillis(value.asInstanceOf[Integer].longValue, precision))
    else {
      throw new InfluxDBMapperException(s"""Unsupported type ${field.getClass} for field ${field.getName}""")
    }

  private def toMillis(value: Long, precision: TimeUnit) = TimeUnit.MILLISECONDS.convert(value, precision)

}
