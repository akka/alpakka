/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.scaladsl.spray

import spray.json.{deserializationError, JsString, JsValue}

import java.time.{Instant, LocalDate, LocalDateTime, LocalTime}
import java.time.format.DateTimeFormatter.{ISO_LOCAL_DATE, ISO_LOCAL_DATE_TIME, ISO_LOCAL_TIME}
import scala.util.Try

/**
 * Provides the BigQueryJsonFormats for [[java.time]] classes.
 */
trait BigQueryJavaTimeFormats {

  implicit object bigQueryLocalDateFormat extends BigQueryJsonFormat[LocalDate] {

    override def read(json: JsValue): LocalDate = json match {
      case BigQueryLocalDate(date) => date
      case x => deserializationError("Expected LocalDate as JsString, but got " + x)
    }

    override def write(date: LocalDate): JsValue =
      JsString(date.format(ISO_LOCAL_DATE))
  }

  private object BigQueryLocalDate {
    def unapply(json: JsString): Option[LocalDate] =
      Try(LocalDate.parse(json.value, ISO_LOCAL_DATE)).toOption
  }

  implicit object bigQueryLocalTimeFormat extends BigQueryJsonFormat[LocalTime] {

    override def read(json: JsValue): LocalTime = json match {
      case BigQueryLocalTime(time) => time
      case x => deserializationError("Expected LocalTime as JsString, but got " + x)
    }

    override def write(time: LocalTime): JsValue =
      JsString(time.format(ISO_LOCAL_TIME))

  }

  private object BigQueryLocalTime {
    def unapply(json: JsString): Option[LocalTime] =
      Try(LocalTime.parse(json.value, ISO_LOCAL_TIME)).toOption
  }

  implicit object bigQueryLocalDateTimeFormat extends BigQueryJsonFormat[LocalDateTime] {

    override def read(json: JsValue): LocalDateTime = json match {
      case BigQueryLocalDateTime(dateTime) => dateTime
      case x => deserializationError("Expected LocalDateTime as JsString, but got " + x)
    }

    override def write(dateTime: LocalDateTime): JsValue =
      JsString(dateTime.format(ISO_LOCAL_DATE_TIME))
  }

  private object BigQueryLocalDateTime {
    def unapply(json: JsString): Option[LocalDateTime] =
      Try(LocalDateTime.parse(json.value, ISO_LOCAL_DATE_TIME)).toOption
  }

  implicit object bigQueryInstantFormat extends BigQueryJsonFormat[Instant] {

    override def read(json: JsValue): Instant = json match {
      case BigQueryInstant(instant) => instant
      case x => deserializationError("Expected Instant as JsString, but got " + x)
    }

    override def write(instant: Instant): JsValue =
      JsString(instant.toString)
  }

  private object BigQueryInstant {
    def unapply(json: JsString): Option[Instant] =
      Try(Instant.ofEpochMilli((BigDecimal(json.value) * 1000).toLongExact)).toOption
  }

}
