/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.javadsl
import java.util.function

import akka.NotUsed
import akka.actor.ActorSystem
import akka.annotation.ApiMayChange
import akka.stream.Materializer
import akka.stream.alpakka.googlecloud.bigquery.BigQueryConfig
import akka.stream.alpakka.googlecloud.bigquery.scaladsl.{BigQueryCallbacks => BigQueryCallbacksScala}
import akka.stream.alpakka.googlecloud.bigquery.impl.parser.Parser.PagingInfo

@ApiMayChange(issue = "https://github.com/akka/alpakka/issues/2353")
object BigQueryCallbacks {
  import scala.compat.java8.FunctionConverters._

  val ignore: function.Function[PagingInfo, NotUsed] = BigQueryCallbacksScala.ignore.asJava
  def tryToStopJob(projectConfig: BigQueryConfig,
                   actorSystem: ActorSystem,
                   materializer: Materializer
  ): function.Function[PagingInfo, NotUsed] =
    BigQueryCallbacksScala.tryToStopJob(projectConfig)(actorSystem, materializer).asJava
}
