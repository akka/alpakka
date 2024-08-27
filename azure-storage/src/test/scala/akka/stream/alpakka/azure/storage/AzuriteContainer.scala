/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka
package azure
package storage

import com.dimafeng.testcontainers.GenericContainer

class AzuriteContainer
    extends GenericContainer(
      "mcr.microsoft.com/azure-storage/azurite:latest",
      exposedPorts = Seq(10000, 10001, 10002)
    ) {

  def getBlobHostAddress: String = s"http://${container.getContainerIpAddress}:${container.getMappedPort(10000)}"

  def getQueueHostAddress: String = s"http://${container.getContainerIpAddress}:${container.getMappedPort(10001)}"

  def getTableHostAddress: String = s"http://${container.getContainerIpAddress}:${container.getMappedPort(10002)}"
}
