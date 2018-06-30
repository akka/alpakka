/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.postgresqlcdc.scaladsl

import akka.NotUsed
import akka.stream.alpakka.postgresqlcdc.{ChangeSet, PostgreSQLInstance, PostgreSQLSourceStage}
import akka.stream.scaladsl.Source

object ChangeDataCapture {

  def source(instance: PostgreSQLInstance): Source[ChangeSet, NotUsed] =
    Source.fromGraph(new PostgreSQLSourceStage(instance))

}
