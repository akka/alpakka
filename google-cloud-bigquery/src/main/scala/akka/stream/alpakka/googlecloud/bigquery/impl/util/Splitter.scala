/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.impl.util

import akka.NotUsed
import akka.annotation.InternalApi
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL}
import akka.stream.{Graph, UniformFanOutShape}

@InternalApi
private[impl] object Splitter {

  def apply[T](out0Predicate: T => Boolean)(
      out1Predicate: T => Boolean = (elem: T) => !out0Predicate(elem)
  ): Graph[UniformFanOutShape[T, T], NotUsed] = GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._

    val broadcast = builder.add(Broadcast[T](2, true))
    val filterOut0 = builder.add(Flow[T].filter(out0Predicate(_)))
    val filterOut1 = builder.add(Flow[T].filter(out1Predicate(_)))

    broadcast.out(0) ~> filterOut0
    broadcast.out(1) ~> filterOut1

    UniformFanOutShape(broadcast.in, filterOut0.out, filterOut1.out)
  }

}
