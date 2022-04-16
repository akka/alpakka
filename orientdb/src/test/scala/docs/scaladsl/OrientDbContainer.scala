/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import com.dimafeng.testcontainers.GenericContainer

class OrientDbContainer
    extends GenericContainer(
      "orientdb:3.1.9",
      exposedPorts = List(2424),
      command = List("/orientdb/bin/server.sh", "-Dmemory.chunk.size=268435456"),
      env = Map(
        "ORIENTDB_ROOT_PASSWORD" -> "root"
      )
    ) {
  def url: String = {
    s"remote:${container.getHost}:${container.getMappedPort(2424)}/"
  }
}
