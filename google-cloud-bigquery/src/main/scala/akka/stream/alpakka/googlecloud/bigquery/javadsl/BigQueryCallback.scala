/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.javadsl

import akka.Done
import akka.actor.ClassicActorSystemProvider
import akka.japi.Function
import akka.stream.alpakka.googlecloud.bigquery.BigQuerySettings
import akka.stream.alpakka.googlecloud.bigquery.model.JobJsonProtocol.JobReference
import akka.stream.alpakka.googlecloud.bigquery.scaladsl.{BigQueryCallback => ScalaCallback}

import java.util
import java.util.concurrent.CompletionStage
import scala.compat.java8.FutureConverters._
import scala.compat.java8.OptionConverters._
import scala.concurrent.Future

object BigQueryCallback {

  private val completedDone: CompletionStage[Done] = Future.successful(Done: Done).toJava

  def ignore[T]: Function[T, CompletionStage[Done]] = _ => completedDone

  def cancelJob(system: ClassicActorSystemProvider,
                settings: BigQuerySettings): Function[util.Optional[JobReference], CompletionStage[Done]] = {
    val callback = ScalaCallback.cancelJob(system, settings)
    jobReference => callback(jobReference.asScala).toJava
  }
}
