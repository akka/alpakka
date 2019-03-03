/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.dynamodb

import com.amazonaws.auth.AWSCredentialsProvider

abstract class AwsClientSettings {
  val region: String
  val host: String
  val port: Int
  val parallelism: Int
  val credentialsProvider: AWSCredentialsProvider
}
