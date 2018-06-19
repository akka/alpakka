/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.reference.scaladsl

import akka.NotUsed
import akka.stream.alpakka.reference.{ReferenceReadMessage, ReferenceWriteMessage, SourceSettings}
import akka.stream.scaladsl.{Flow, Source}

import scala.concurrent.Future

object Reference {

  def source(settings: SourceSettings): Source[ReferenceReadMessage, Future[NotUsed]] = ???

  def flow(): Flow[ReferenceWriteMessage, ReferenceWriteMessage, NotUsed] = ???
}
