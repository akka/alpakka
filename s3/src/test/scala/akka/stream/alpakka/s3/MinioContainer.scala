/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.s3

import com.dimafeng.testcontainers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait

import java.time.Duration

class MinioContainer(accessKey: String, secretKey: String, domain: String)
    extends GenericContainer(
      // https://hub.docker.com/r/firstfinger/minio/tags
      "firstfinger/minio:1.7.3",
      exposedPorts = List(9000),
      waitStrategy = Some(Wait.forHttp("/minio/health/ready").forPort(9000).withStartupTimeout(Duration.ofSeconds(10))),
      command = List("server", "/data"),
      env = Map(
        "MINIO_ACCESS_KEY" -> accessKey,
        "MINIO_SECRET_KEY" -> secretKey,
        "MINIO_DOMAIN" -> domain
      )
    ) {

  def getHostAddress: String =
    s"http://${container.getContainerIpAddress}:${container.getMappedPort(9000)}"

  def getVirtualHost: String =
    s"http://{bucket}.$domain:${container.getMappedPort(9000)}"

}
