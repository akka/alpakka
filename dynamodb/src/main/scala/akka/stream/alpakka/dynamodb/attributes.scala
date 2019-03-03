/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.dynamodb
import akka.annotation.InternalApi
import akka.stream.Attributes
import akka.stream.Attributes.Attribute

/**
 * Akka Stream attributes that are used when materializing DynamoDb stream blueprints.
 */
object DynamoAttributes {

  /**
   * Client to use for the DynamoDb stream
   */
  def client(client: DynamoClient): Attributes = Attributes(new Client(client))

  final class Client @InternalApi private[DynamoAttributes] (val client: DynamoClient) extends Attribute
}
