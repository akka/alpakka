/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.postgresqlcdc.javadsl

import akka.NotUsed
import akka.stream.alpakka.postgresqlcdc.{ChangeDataCaptureSettings, ChangeSet}
import akka.stream.javadsl.Source
import akka.stream.alpakka.postgresqlcdc._

object ChangeDataCapture {

  def source(instance: PostgreSQLInstance, settings: ChangeDataCaptureSettings): Source[ChangeSet, NotUsed] =
    scaladsl.ChangeDataCapture.source(instance, settings).asJava

}
