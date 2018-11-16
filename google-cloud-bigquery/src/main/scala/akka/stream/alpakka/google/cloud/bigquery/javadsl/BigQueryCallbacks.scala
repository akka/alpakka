/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.google.cloud.bigquery.javadsl
import java.util.function

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.alpakka.google.cloud.bigquery.BigQueryFlowModels.BigQueryProjectConfig
import akka.stream.alpakka.google.cloud.bigquery.scaladsl.{BigQueryCallbacks => BigQueryCallbacksScala}
import akka.stream.alpakka.google.cloud.bigquery.impl.parser.Parser.PagingInfo

object BigQueryCallbacks {
  import scala.compat.java8.FunctionConverters._

  val ignore: function.Function[PagingInfo, NotUsed] = BigQueryCallbacksScala.ignore.asJava
  def tryToStopJob(projectConfig: BigQueryProjectConfig,
                   actorSystem: ActorSystem,
                   materializer: Materializer): function.Function[PagingInfo, NotUsed] =
    BigQueryCallbacksScala.tryToStopJob(projectConfig)(actorSystem, materializer).asJava
}
