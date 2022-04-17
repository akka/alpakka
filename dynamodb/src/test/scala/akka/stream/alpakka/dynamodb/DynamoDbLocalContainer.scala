/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.dynamodb

import com.dimafeng.testcontainers.GenericContainer

import java.net.URI

class DynamoDbLocalContainer
    extends GenericContainer(
      "deangiberson/aws-dynamodb-local",
      exposedPorts = List(8000)
    ) {

  def uri: URI = new URI(s"http://${container.getHost}:${container.getMappedPort(8000)}/")
}
