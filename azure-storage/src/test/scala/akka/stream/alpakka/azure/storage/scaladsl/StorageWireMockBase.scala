/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka
package azure
package storage
package scaladsl

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers._
import StorageWireMockBase.{config, getCallerName, initServer, AccountName, ETagValue}
import akka.http.scaladsl.model.ContentTypes
import akka.testkit.TestKit
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import com.github.tomakehurst.wiremock.extension.requestfilter.{
  RequestFilterAction,
  RequestWrapper,
  StubRequestFilterV2
}
import com.github.tomakehurst.wiremock.http.Request
import com.github.tomakehurst.wiremock.stubbing.{ServeEvent, StubMapping}
import com.typesafe.config.{Config, ConfigFactory}

import scala.util.Try

abstract class StorageWireMockBase(_system: ActorSystem, val _wireMockServer: WireMockServer) extends TestKit(_system) {

  private val port = _wireMockServer.port()
  protected val mock = new WireMock("localhost", port)

  // test data
  protected val containerName = "my-container"
  protected val blobName = "my-blob.txt"
  protected val payload = "The quick brown fox jumps over the lazy dog."
  protected val contentLength: Long = payload.length.toLong
  protected val contentRange: ByteRange.Slice = ByteRange(0, contentLength - 1)
  protected val subRange: ByteRange.Slice = ByteRange(4, 8)

  private def this(mock: WireMockServer) =
    this(
      ActorSystem(getCallerName(StorageWireMockBase.getClass), config(mock.port()).withFallback(ConfigFactory.load())),
      mock
    )

  def this() = {
    this(initServer())
    system.registerOnTermination(stopWireMockServer())
  }

  protected def mockCreateContainer(): StubMapping =
    mock.register(
      put(urlEqualTo(s"/$AccountName/$containerName?restype=container"))
        .willReturn(
          aResponse()
            .withStatus(201)
            .withHeader(ETag.name, ETagValue)
            .withHeader(`Content-Length`.name, "0")
        )
    )

  protected def mockDeleteContainer(): StubMapping =
    mock.register(
      delete(urlEqualTo(s"/$AccountName/$containerName?restype=container"))
        .willReturn(
          aResponse()
            .withStatus(202)
            .withHeader(`Content-Length`.name, "0")
        )
    )

  protected def mockCreateDirectory(): StubMapping =
    mock.register(
      put(urlEqualTo(s"/$AccountName/$containerName?restype=directory"))
        .willReturn(
          aResponse()
            .withStatus(201)
            .withHeader(ETag.name, ETagValue)
            .withHeader(`Content-Length`.name, "0")
        )
    )

  protected def mockDeleteDirectory(): StubMapping =
    mock.register(
      delete(urlEqualTo(s"/$AccountName/$containerName?restype=directory"))
        .willReturn(
          aResponse()
            .withStatus(202)
            .withHeader(`Content-Length`.name, "0")
        )
    )

  protected def mockPutBlockBlob(): StubMapping =
    mock.register(
      put(urlEqualTo(s"/$AccountName/$containerName/$blobName"))
        .withHeader(BlobTypeHeaderKey, equalTo(BlockBlobType))
        .withRequestBody(equalTo(payload))
        .willReturn(
          aResponse()
            .withStatus(201)
            .withHeader(ETag.name, ETagValue)
            .withHeader(`Content-Length`.name, payload.length.toString)
            .withHeader(`Content-Type`.name, "text/plain; charset=UTF-8")
        )
    )

  protected def mockPutPageBlob(): StubMapping =
    mock.register(
      put(urlEqualTo(s"/$AccountName/$containerName/$blobName"))
        .withHeader(BlobTypeHeaderKey, equalTo(PageBlobType))
        .withHeader(PageBlobContentLengthHeaderKey, equalTo("512"))
        .withHeader(PageBlobSequenceNumberHeaderKey, equalTo("0"))
        .willReturn(
          aResponse()
            .withStatus(201)
            .withHeader(ETag.name, ETagValue)
            .withHeader(`Content-Length`.name, "0")
            .withHeader(`Content-Type`.name, "text/plain; charset=UTF-8")
        )
    )

  protected def mockPutAppendBlob(): StubMapping =
    mock.register(
      put(urlEqualTo(s"/$AccountName/$containerName/$blobName"))
        .withHeader(BlobTypeHeaderKey, equalTo(AppendBlobType))
        .willReturn(
          aResponse()
            .withStatus(201)
            .withHeader(ETag.name, ETagValue)
            .withHeader(`Content-Length`.name, "0")
            .withHeader(`Content-Type`.name, "text/plain; charset=UTF-8")
        )
    )

  protected def mockGetBlob(versionId: Option[String] = None, leaseId: Option[String] = None): StubMapping =
    mock.register(
      get(urlPathEqualTo(s"/$AccountName/$containerName/$blobName"))
        .withQueryParam("versionId", toStringValuePattern(versionId))
        .withHeader(LeaseIdHeaderKey, toStringValuePattern(leaseId))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader(ETag.name, ETagValue)
            .withBody(payload)
        )
    )

  protected def mockGetBlobWithServerSideEncryption(): StubMapping =
    mock.register(
      get(urlPathEqualTo(s"/$AccountName/$containerName/$blobName"))
        .withHeader("x-ms-encryption-algorithm", equalTo("AES256"))
        .withHeader("x-ms-encryption-key", equalTo("SGVsbG9Xb3JsZA=="))
        .withHeader("x-ms-encryption-key-sha256", equalTo("hy5OUM6ZkNiwQTMMR8nd0Rvsa1A66ThqmdqFhOm7EsQ="))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader(ETag.name, ETagValue)
            .withBody(payload)
        )
    )

  protected def mockGetBlobWithRange(): StubMapping =
    mock.register(
      get(urlEqualTo(s"/$AccountName/$containerName/$blobName"))
        .withHeader(Range.name, equalTo(s"bytes=${subRange.first}-${subRange.last}"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader(ETag.name, ETagValue)
            .withBody(payload.substring(subRange.first.toInt, subRange.last.toInt + 1))
        )
    )

  protected def mockGetBlobProperties(): StubMapping =
    mock.register(
      head(urlEqualTo(s"/$AccountName/$containerName/$blobName"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader(ETag.name, ETagValue)
            .withHeader(`Content-Length`.name, payload.length.toString)
            .withHeader(`Content-Type`.name, "text/plain; charset=UTF-8")
        )
    )

  protected def mockDeleteBlob(): StubMapping =
    mock.register(
      delete(urlEqualTo(s"/$AccountName/$containerName/$blobName"))
        .willReturn(
          aResponse()
            .withStatus(202)
            .withHeader(ETag.name, ETagValue)
        )
    )

  protected def mockCreateFile(): StubMapping =
    mock.register(
      put(urlEqualTo(s"/$AccountName/$containerName/$blobName"))
        .withHeader(XMsContentLengthHeaderKey, equalTo(contentLength.toString))
        .withHeader(FileTypeHeaderKey, equalTo("file"))
        .willReturn(
          aResponse()
            .withStatus(201)
            .withHeader(ETag.name, ETagValue)
            .withHeader(`Content-Length`.name, "0")
        )
    )

  protected def mockUpdateRange(): StubMapping =
    mock.register(
      put(urlEqualTo(s"/$AccountName/$containerName/$blobName?comp=range"))
        .withHeader(Range.name, equalTo(s"bytes=0-${contentLength - 1}"))
        .withHeader(FileWriteTypeHeaderKey, equalTo("update"))
        .withRequestBody(equalTo(payload))
        .willReturn(
          aResponse()
            .withStatus(201)
            .withHeader(ETag.name, ETagValue)
            .withHeader(`Content-Length`.name, "0")
        )
    )

  protected def mockClearRange(): StubMapping =
    mock.register(
      put(urlEqualTo(s"/$AccountName/$containerName/$blobName?comp=range"))
        .withHeader(`Content-Type`.name, equalTo(ContentTypes.NoContentType.toString()))
        .withHeader(Range.name, equalTo(s"bytes=${subRange.first}-${subRange.last}"))
        .withHeader(FileWriteTypeHeaderKey, equalTo("clear"))
        .willReturn(
          aResponse()
            .withStatus(201)
            .withHeader(ETag.name, ETagValue)
            .withHeader(`Content-Length`.name, "0")
        )
    )

  protected def mock404s(): StubMapping =
    mock.register(
      any(anyUrl())
        .willReturn(aResponse().withStatus(404).withBody("""
          |<Error>
          | <Code>ResourceNotFound</Code>
          | <Message>The specified resource doesn't exist.</Message>
          |</Error>
          |""".stripMargin))
    )

  private def toStringValuePattern(maybeValue: Option[String]) = maybeValue.map(equalTo).getOrElse(absent())

  private def stopWireMockServer(): Unit = _wireMockServer.stop()
}

object StorageWireMockBase {

  val AccountName = "teststoreaccount"
  val ETagRawValue = "fba9dede5f27731c9771645a39863328"
  val ETagValue = s""""$ETagRawValue""""

  def initServer(): WireMockServer = {
    val server = new WireMockServer(
      wireMockConfig()
        .extensions(new RemoveDuplicateContentLengthHeader())
        .dynamicPort()
    )
    server.start()
    server
  }

  def getCallerName(clazz: Class[_]): String = {
    val s = (Thread.currentThread.getStackTrace map (_.getClassName) drop 1)
      .dropWhile(_ matches "(java.lang.Thread|.*WireMockBase.?$)")
    val reduced = s.lastIndexWhere(_ == clazz.getName) match {
      case -1 => s
      case z => s drop (z + 1)
    }
    reduced.head.replaceFirst(""".*\.""", "").replaceAll("[^a-zA-Z_0-9]", "_")
  }

  def config(proxyPort: Int): Config = ConfigFactory.parseString(s"""
       |akka.http.client.log-unencrypted-network-bytes = 1000
       |akka.http.parsing.max-to-strict-bytes=infinite
       |${StorageSettings.ConfigPath} {
       | endpoint-url = "http://localhost:$proxyPort"
       | credentials {
       |    authorization-type = anon
       |    account-name = $AccountName
       | }
       |}
       |""".stripMargin)

  private class RemoveDuplicateContentLengthHeader extends StubRequestFilterV2 {
    override def filter(request: Request, serveEvent: ServeEvent): RequestFilterAction = {
      val headerName = `Content-Length`.name

      val updatedRequest =
        Try(request.getHeader(headerName)).toOption match {
          case Some(contentLengthValue) =>
            RequestWrapper
              .create()
              .removeHeader(headerName)
              .addHeader(headerName, contentLengthValue)
              .wrap(request)

          case None => request
        }

      RequestFilterAction.continueWith(updatedRequest)
    }

    override def getName: String = "remove-duplicate-content-length-headers"
  }
}
