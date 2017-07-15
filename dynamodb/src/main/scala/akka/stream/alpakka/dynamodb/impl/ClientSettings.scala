/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.dynamodb.impl

abstract class ClientSettings {
  val region: String
  val host: String
  val port: Int
  val parallelism: Int
}
