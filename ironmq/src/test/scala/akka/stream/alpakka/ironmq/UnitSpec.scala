/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.ironmq

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Matchers, WordSpec}

import scala.concurrent.duration._

class UnitSpec extends WordSpec with Matchers with ScalaFutures {
  override implicit val patienceConfig: PatienceConfig = PatienceConfig(timeout = 15.seconds, interval = 1.second)
}
