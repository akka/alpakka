/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.reference.scaladsl

import akka.{Done, NotUsed}
import akka.stream.alpakka.reference.impl.{ReferenceFlow, ReferenceSource}
import akka.stream.alpakka.reference.{ReferenceReadResult, ReferenceWriteMessage, ReferenceWriteResult, SourceSettings}
import akka.stream.scaladsl.{Flow, Source}

import scala.concurrent.{ExecutionContext, Future}

object Reference {

  /**
   * API doc should describe where the messages are coming from.
   *
   * Also describe the significance of the materialized value.
   */
  def source(settings: SourceSettings): Source[ReferenceReadResult, Future[Done]] =
    Source.fromGraph(new ReferenceSource(settings))

  /**
   * API doc should describe what will be done to the incoming messages to the flow,
   * and what messages will be emitted by the flow.
   */
  def flow(): Flow[ReferenceWriteMessage, ReferenceWriteResult, NotUsed] =
    Flow.fromGraph(new ReferenceFlow())

  /**
   * If the operator needs an ExecutionContext, take it as an implicit parameter.
   */
  def flowAsyncMapped()(implicit ec: ExecutionContext): Flow[ReferenceWriteMessage, ReferenceWriteResult, NotUsed] =
    flow().mapAsync(parallelism = 4)(m => Future { m })
}
