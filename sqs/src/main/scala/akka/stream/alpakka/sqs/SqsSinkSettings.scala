/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.sqs

object SqsSinkSettings {
  val Defaults = SqsSinkSettings(maxInFlight = 10)
}

//#SqsSinkSettings
final case class SqsSinkSettings(maxInFlight: Int) {
  require(maxInFlight > 0)
}
//#SqsSinkSettings
