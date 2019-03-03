/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.dynamodb.impl

import akka.NotUsed
import akka.annotation.InternalApi
import akka.stream.SourceShape
import akka.stream.alpakka.dynamodb.{AwsOp, AwsPagedOp}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Merge, Source}
import com.amazonaws.{AmazonWebServiceResult, ResponseMetadata}

/**
 * INTERNAL API
 */
@InternalApi
private[dynamodb] object Paginator {

  def source(flow: Flow[AwsOp, AmazonWebServiceResult[ResponseMetadata], NotUsed],
             op: AwsPagedOp): Source[op.B, NotUsed] = {
    val next = Flow
      .fromFunction[op.B, Option[AwsPagedOp]](op.next(op.request, _))
      .takeWhile(_.isDefined)
      .map(_.get)
    val cast = Flow
      .fromFunction[AmazonWebServiceResult[ResponseMetadata], op.B](_.asInstanceOf[op.B])
    Source.fromGraph(GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      val merge = b.add(Merge[AwsPagedOp](2))
      val bcast = b.add(Broadcast[op.B](2))

      Source.single(op) ~> merge ~> flow ~> cast ~> bcast
      bcast.out(0) ~> next ~> merge

      SourceShape(bcast.out(1))
    })
  }

}
