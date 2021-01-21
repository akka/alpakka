/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.e2e
import akka.actor.ActorSystem
import akka.stream.alpakka.googlecloud.bigquery.HoverflySupport
import akka.stream.alpakka.googlecloud.bigquery.e2e.BigQueryEndToEndSpec.{A, B, C}
import akka.testkit.TestKit
import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.annotation.{JsonCreator, JsonInclude, JsonProperty, JsonPropertyOrder}
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer
import io.specto.hoverfly.junit.core.{HoverflyMode, SimulationSource}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpecLike

import java.io.File
import scala.collection.JavaConverters._
import scala.util.Random

class BigQueryEndToEndSpec
    extends TestKit(ActorSystem("BigQueryEndToEndSpec"))
    with AsyncWordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with HoverflySupport {

  override def beforeAll(): Unit = {
    super.beforeAll()
    system.settings.config.getString("alpakka.google.bigquery.test.e2e-mode") match {
      case "simulate" =>
        hoverfly.simulate(SimulationSource.url(getClass.getClassLoader.getResource("BigQueryEndToEndSpec.json")))
      case "capture" => hoverfly.resetMode(HoverflyMode.CAPTURE)
      case _ => throw new IllegalArgumentException
    }
  }

  override def afterAll() = {
    system.terminate()
    if (hoverfly.getMode == HoverflyMode.CAPTURE)
      hoverfly.exportSimulation(new File("hoverfly/BigQueryEndToEndSpec.json").toPath)
    super.afterAll()
  }

  implicit def scheduler = system.scheduler

  val rng = new Random(1234567890)

  val datasetId = f"e2e_dataset_${rng.nextInt(1000)}%03d"
  val tableId = f"e2e_table_${rng.nextInt(1000)}%03d"

  def randomC(): C = C(BigDecimal(f"${rng.nextInt(100)}.${rng.nextInt(100)}%02d"))
  def randomB(): B = B(
    if (rng.nextBoolean()) Some(rng.nextString(rng.nextInt(64))) else None,
    Seq.fill(rng.nextInt(16))(randomC())
  )
  def randomA(): A = A(
    rng.nextInt(),
    rng.nextLong(),
    rng.nextFloat(),
    rng.nextDouble(),
    rng.nextString(rng.nextInt(64)),
    rng.nextBoolean(),
    randomB()
  )

  val rows = List.fill(10)(randomA())

}

object BigQueryEndToEndSpec {

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

  case class C(numeric: BigDecimal) {
    def this(node: JsonNode) =
      this(BigDecimal(node.get("f").get(0).get("v").textValue()))
    @JsonSerialize(using = classOf[ToStringSerializer])
    def getNumeric = numeric
  }

}
