/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.google.cloud.bigquery.impl.util

import akka.NotUsed
import akka.stream.scaladsl.{Concat, GraphDSL, Source}
import akka.stream.{FlowShape, Graph}

object FlowInitializer {
  def apply[T](initialValue: T): Graph[FlowShape[T, T], NotUsed] = GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._

    val concat = builder.add(Concat[T](2))

    Source.single(initialValue) ~> concat.in(0)

    new FlowShape(concat.in(1), concat.out)
  }
}
