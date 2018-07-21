/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.dynamodb.impl

import com.amazonaws.auth.AWSCredentialsProvider

abstract class ClientSettings {
  val region: String
  val host: String
  val port: Int
  val parallelism: Int
  val credentialsProvider: AWSCredentialsProvider
}
