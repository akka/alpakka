/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka

package object jms {
  @deprecated("Use JmsConsumerSettings instead", "0.18")
  type JmsSourceSettings = JmsConsumerSettings
  @deprecated("Use JmsConsumerSettings instead", "0.18")
  val JmsSourceSettings: JmsConsumerSettings.type = JmsConsumerSettings

  @deprecated("Use JmsProducerSettings instead", "0.18")
  type JmsSinkSettings = JmsProducerSettings
  @deprecated("Use JmsProducerSettings instead", "0.18")
  val JmsSinkSettings: JmsProducerSettings.type = JmsProducerSettings
}
