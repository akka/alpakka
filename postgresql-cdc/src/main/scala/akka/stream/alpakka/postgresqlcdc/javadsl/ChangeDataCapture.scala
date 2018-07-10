/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.postgresqlcdc.javadsl

import akka.NotUsed
import akka.stream.alpakka.postgresqlcdc.{ChangeSet, PgCdcSourceSettings, _}
import akka.stream.javadsl.{Sink, Source}

object ChangeDataCapture {

  def source(instance: PostgreSQLInstance, settings: PgCdcSourceSettings): Source[ChangeSet, NotUsed] =
    scaladsl.ChangeDataCapture.source(instance, settings).asJava

  def ackSink(instance: PostgreSQLInstance, settings: PgCdcAckSinkSettings): Sink[ChangeSet, NotUsed] =
    scaladsl.ChangeDataCapture.ackSink(instance, settings).asJava

}
