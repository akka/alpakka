/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google.util

import akka.annotation.InternalApi
import akka.stream.FlowShape
import akka.stream.scaladsl.{Flow, GraphDSL, Keep, Merge, Partition}
import com.github.ghik.silencer.silent

@InternalApi
private[google] object EitherFlow {

  def apply[LeftIn, LeftOut, LeftMat, RightIn, RightOut, RightMat](
      leftFlow: Flow[LeftIn, LeftOut, LeftMat],
      rightFlow: Flow[RightIn, RightOut, RightMat]
  ): Flow[Either[LeftIn, RightIn], Either[LeftOut, RightOut], (LeftMat, RightMat)] =
    Flow.fromGraph(
      GraphDSL.create(leftFlow, rightFlow)(Keep.both) { implicit b => (leftFlow, rightFlow) =>
        import GraphDSL.Implicits._
        val in = b.add(Partition[Either[LeftIn, RightIn]](2, x => if (x.isRight) 1 else 0))
        val out = b.add(Merge[Either[LeftOut, RightOut]](2))
        in ~> Flow[Either[LeftIn, RightIn]].map(_.left.get) ~> leftFlow ~> Flow[LeftOut].map(Left(_)) ~> out
        in ~> Flow[Either[LeftIn, RightIn]].map(_.right.get: @silent("deprecated")) ~> rightFlow ~> Flow[RightOut].map(
          Right(_)
        ) ~> out
        FlowShape(in.in, out.out)
      }
    )

}
