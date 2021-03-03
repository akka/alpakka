/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.e2e

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.annotation.{JsonCreator, JsonInclude, JsonProperty, JsonPropertyOrder}
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer

import java.time.{Instant, LocalDate, LocalDateTime, LocalTime}
import scala.collection.JavaConverters._

@JsonPropertyOrder(alphabetic = true)
case class A(integer: Int, long: Long, float: Float, double: Double, string: String, boolean: Boolean, record: B) {

  @JsonCreator
  def this(@JsonProperty("f") f: JsonNode) =
    this(
      f.get(0).get("v").textValue().toInt,
      f.get(1).get("v").textValue().toLong,
      f.get(2).get("v").textValue().toFloat,
      f.get(3).get("v").textValue().toDouble,
      f.get(4).get("v").textValue(),
      f.get(5).get("v").textValue().toBoolean,
      new B(f.get(6).get("v"))
    )

  def getInteger = integer
  @JsonSerialize(using = classOf[ToStringSerializer])
  def getLong = long
  def getFloat = float
  def getDouble = double
  def getString = string
  def getBoolean = boolean
  def getRecord = record

}

@JsonPropertyOrder(alphabetic = true)
@JsonInclude(Include.NON_NULL)
case class B(nullable: Option[String], repeated: Seq[C]) {
  def this(node: JsonNode) =
    this(
      Option(node.get("f").get(0).get("v").textValue()),
      node.get("f").get(1).get("v").asScala.map(n => new C(n.get("v"))).toList
    )

  def getNullable = nullable.orNull
  def getRepeated = repeated.asJava
}

@JsonPropertyOrder(alphabetic = true)
case class C(numeric: BigDecimal, date: LocalDate, time: LocalTime, dateTime: LocalDateTime, timestamp: Instant) {

  def this(node: JsonNode) =
    this(
      BigDecimal(node.get("f").get(0).get("v").textValue()),
      LocalDate.parse(node.get("f").get(1).get("v").textValue()),
      LocalTime.parse(node.get("f").get(2).get("v").textValue()),
      LocalDateTime.parse(node.get("f").get(3).get("v").textValue()),
      Instant.ofEpochMilli((BigDecimal(node.get("f").get(4).get("v").textValue()) * 1000).toLong)
    )

  @JsonSerialize(using = classOf[ToStringSerializer])
  def getNumeric = numeric
  def getDate = date
  def getTime = time
  def getDateTime = dateTime
  def getTimestamp = timestamp
}
