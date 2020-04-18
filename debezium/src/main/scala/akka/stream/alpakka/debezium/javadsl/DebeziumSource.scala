/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.debezium.javadsl

import java.util.concurrent.{ExecutorService, Executors}

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.alpakka.debezium.{CommittableRecord, DebeziumSettings, SimpleRecord}
import akka.stream.javadsl._

import akka.stream.alpakka.debezium.scaladsl.{DebeziumSource => ScalaDebeziumSource}

object DebeziumSource {
  def committable(settings: DebeziumSettings,
                  actorSystem: ActorSystem,
                  executor: ExecutorService = Executors.newSingleThreadExecutor()): Source[CommittableRecord, NotUsed] =
    ScalaDebeziumSource.committable(settings, executor)(actorSystem).asJava

  def apply(settings: DebeziumSettings,
            actorSystem: ActorSystem,
            executor: ExecutorService = Executors.newSingleThreadExecutor()): Source[SimpleRecord, NotUsed] =
    ScalaDebeziumSource(settings, executor)(actorSystem).asJava
}
