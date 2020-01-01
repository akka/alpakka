/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms

import akka.annotation.DoNotInherit
import javax.jms

/**
 * Shared settings for all JMS stages.
 * Used for internal standardization, and not meant to be used by user code.
 */
@DoNotInherit
trait JmsSettings {
  def connectionFactory: jms.ConnectionFactory
  def connectionRetrySettings: ConnectionRetrySettings
  def destination: Option[Destination]
  def credentials: Option[Credentials]
  def sessionCount: Int
  def connectionStatusSubscriptionTimeout: scala.concurrent.duration.FiniteDuration
}
