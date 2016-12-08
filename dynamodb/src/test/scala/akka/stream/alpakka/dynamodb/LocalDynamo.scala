/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.dynamodb

import akka.stream.alpakka.dynamodb.impl.DynamoSettings
import com.amazonaws.services.dynamodbv2.local.main.ServerRunner
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer

class LocalDynamo(settings: DynamoSettings) {

  private val serverArgs = Array("-port", settings.port.toString, "-inMemory")
  private val server: DynamoDBProxyServer = ServerRunner.createServerFromCommandLineArgs(serverArgs)

  def start(): Unit = server.start()

  def stop(): Unit = server.stop()

}
