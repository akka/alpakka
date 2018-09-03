/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.postgresqlcdc.scaladsl

import akka.NotUsed
import akka.stream.alpakka.postgresqlcdc._
import akka.stream.alpakka.postgresqlcdc.impl.{PostgreSQLAckSinkStage, PostgreSQLSourceStage}
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source

object ChangeDataCapture {

  def source(instance: PostgreSQLInstance, settings: PgCdcSourceSettings): Source[ChangeSet, NotUsed] =
    Source.fromGraph(new PostgreSQLSourceStage(instance, settings))

  def ackSink(instance: PostgreSQLInstance, settings: PgCdcAckSinkSettings): Sink[AckLogSeqNum, NotUsed] =
    Sink.fromGraph(new PostgreSQLAckSinkStage(instance, settings))

}
