/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.reference.scaladsl

import akka.NotUsed
import akka.stream.alpakka.reference.impl.{ReferenceFlow, ReferenceSource}
import akka.stream.alpakka.reference.{ReferenceReadMessage, ReferenceWriteMessage, SourceSettings}
import akka.stream.scaladsl.{Flow, Source}

import scala.concurrent.Future

object Reference {

  /**
   * API doc should describe where the messages are coming from.
   *
   * Also describe the significance of the materialized value.
   */
  def source(settings: SourceSettings): Source[ReferenceReadMessage, Future[NotUsed]] =
    Source.fromGraph(new ReferenceSource(settings))

  /**
   * API doc should describe what will be done to the incoming messages to the flow,
   * and what messages will be emitted by the flow.
   */
  def flow(): Flow[ReferenceWriteMessage, ReferenceWriteMessage, NotUsed] =
    Flow.fromGraph(new ReferenceFlow())
}
