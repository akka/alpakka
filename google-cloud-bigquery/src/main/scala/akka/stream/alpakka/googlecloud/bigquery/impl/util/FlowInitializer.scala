/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.impl.util

import akka.NotUsed
import akka.stream.scaladsl.{Concat, GraphDSL, Source}
import akka.stream.{FlowShape, Graph}

private[impl] object FlowInitializer {
  def apply[T](initialValue: T): Graph[FlowShape[T, T], NotUsed] = GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._

    val concat = builder.add(Concat[T](2))

    Source.single(initialValue) ~> concat.in(0)

    new FlowShape(concat.in(1), concat.out)
  }
}
