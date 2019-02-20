/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package playground

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}

object KafkaEmbedded {

  def start(): Unit =
    start(19000)

  def start(kafkaPort: Int): Unit =
    EmbeddedKafka.start()(EmbeddedKafkaConfig(kafkaPort))

  def stop() =
    EmbeddedKafka.stop()
}
