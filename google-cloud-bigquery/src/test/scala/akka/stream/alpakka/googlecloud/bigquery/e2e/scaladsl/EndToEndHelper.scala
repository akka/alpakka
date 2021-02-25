/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.e2e.scaladsl

import akka.stream.alpakka.googlecloud.bigquery.e2e.{A, B, C}

import scala.util.Random

trait EndToEndHelper {

  private val rng = new Random(1234567890)

  val datasetId = f"e2e_dataset_${rng.nextInt(1000)}%03d"

  val tableId = f"e2e_table_${rng.nextInt(1000)}%03d"

  private def randomC(): C = C(BigDecimal(f"${rng.nextInt(100)}.${rng.nextInt(100)}%02d"))

  private def randomB(): B = B(
    if (rng.nextBoolean()) Some(rng.nextString(rng.nextInt(64))) else None,
    Seq.fill(rng.nextInt(16))(randomC())
  )

  private def randomA(): A = A(
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