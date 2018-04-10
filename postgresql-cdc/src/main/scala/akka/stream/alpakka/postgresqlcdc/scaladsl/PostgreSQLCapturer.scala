/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.postgresqlcdc.scaladsl

import akka.NotUsed
import akka.stream.alpakka.postgresqlcdc.{
  ChangeSet,
  PgSQLChangeDataCaptureSourceStage,
  PostgreSQLChangeDataCaptureSettings
}
import akka.stream.scaladsl.Source

object PostgreSQLCapturer {

  /**
   * Scala API: creates a [[PostgreSQLCapturer]]
   */
  def apply(settings: PostgreSQLChangeDataCaptureSettings): Source[ChangeSet, NotUsed] =
    Source.fromGraph(new PgSQLChangeDataCaptureSourceStage(settings))
}
