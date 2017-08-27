/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.ironmq

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.{BeforeAndAfterEach, Suite}

trait ConfigFixture extends BeforeAndAfterEach { _: Suite =>

  private var mutableConfig = Option.empty[Config]
  def config: Config = mutableConfig.getOrElse(throw new IllegalStateException("Config not initialized"))

  protected def initConfig(): Config = ConfigFactory.load()

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    mutableConfig = Option(initConfig())
  }

}
