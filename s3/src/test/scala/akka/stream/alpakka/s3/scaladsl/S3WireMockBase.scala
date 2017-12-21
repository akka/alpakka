/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.s3.scaladsl

import akka.actor.ActorSystem
import akka.stream.alpakka.s3.impl.ServerSideEncryption
import akka.stream.alpakka.s3.scaladsl.S3WireMockBase._
import akka.testkit.TestKit
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration._
import com.github.tomakehurst.wiremock.matching.EqualToPattern

abstract class S3WireMockBase(_system: ActorSystem, _wireMockServer: WireMockServer) extends TestKit(_system) {

  def this(mock: WireMockServer) = this(ActorSystem(getCallerName(getClass)), mock)
  def this() = this(initServer())

  val mock = new WireMock("localhost", _wireMockServer.port())
  val port = _wireMockServer.port()

  def mock404s(): Unit =
    mock.register(
      any(anyUrl()).willReturn(
        aResponse()
          .withStatus(404)
          .withBody(
            "<Error><Code>NoSuchKey</Code><Message>No key found</Message>" +
            "<RequestId>XXXX</RequestId><HostId>XXXX</HostId></Error>"
          )
      )
    )

  def mockSSEInvalidRequest(): Unit =
    mock.register(
      any(anyUrl()).willReturn(
        aResponse()
          .withStatus(400)
          .withBody(
            "<Error><Code>InvalidRequest</Code><Message>The object was stored using a form of Server Side Encryption. " +
            "The correct parameters must be provided to retrieve the object.</Message>" +
            "<RequestId>XXX</RequestId>" +
            "<HostId>XXXX</HostId></Error>"
          )
      )
    )

  def stopWireMockServer(): Unit = _wireMockServer.stop()

  val body = "<response>Some content</response>"
  val bodySSE = "<response>Some other content</response>"
  val bucketKey = "testKey"
  val bucket = "testBucket"
  val uploadId = "VXBsb2FkIElEIGZvciA2aWWpbmcncyBteS1tb3ZpZS5tMnRzIHVwbG9hZA"
  val etag = "5b27a21a97fcf8a7004dd1d906e7a5ba"
  val etagSSE = "5b27a21a97fcf8a7004dd1d906e7a5cd"
  val url = s"http://testbucket.s3.amazonaws.com/testKey"
  val (bytesRangeStart, bytesRangeEnd) = (2, 10)
  val rangeOfBody = body.getBytes.slice(bytesRangeStart, bytesRangeEnd + 1)
  val rangeOfBodySSE = bodySSE.getBytes.slice(bytesRangeStart, bytesRangeEnd + 1)
  val listPrefix = "testPrefix"
  val listKey = "testingKey.txt"

  val sseCustomerKey = "key"
  val sseCustomerMd5Key = "md5"
  val sseCustomerKeys = ServerSideEncryption.CustomerKeys(sseCustomerKey, Some(sseCustomerMd5Key))

  def mockDownload(): Unit =
    mock
      .register(
        get(urlEqualTo(s"/$bucketKey")).willReturn(
          aResponse().withStatus(200).withHeader("ETag", """"fba9dede5f27731c9771645a39863328"""").withBody(body)
        )
      )

  def mockDownloadSSEC(): Unit =
    mock
      .register(
        get(urlEqualTo(s"/$bucketKey"))
          .withHeader("x-amz-server-side-encryption-customer-algorithm", new EqualToPattern("AES256"))
          .withHeader("x-amz-server-side-encryption-customer-key", new EqualToPattern(sseCustomerKey))
          .withHeader("x-amz-server-side-encryption-customer-key-MD5", new EqualToPattern(sseCustomerMd5Key))
          .willReturn(
            aResponse()
              .withStatus(200)
              .withHeader("ETag", """"fba9dede5f27731c9771645a39863328"""")
              .withBody(bodySSE)
          )
      )

  def mockHead(): Unit =
    mock
      .register(
        head(urlEqualTo(s"/$bucketKey")).willReturn(
          aResponse().withStatus(200).withHeader("ETag", s""""$etag"""")
        )
      )

  def mockHeadSSEC(): Unit =
    mock
      .register(
        head(urlEqualTo(s"/$bucketKey"))
          .withHeader("x-amz-server-side-encryption-customer-algorithm", new EqualToPattern("AES256"))
          .withHeader("x-amz-server-side-encryption-customer-key", new EqualToPattern(sseCustomerKey))
          .withHeader("x-amz-server-side-encryption-customer-key-MD5", new EqualToPattern(sseCustomerMd5Key))
          .willReturn(
            aResponse().withStatus(200).withHeader("ETag", s""""$etagSSE"""")
          )
      )

  def mockRangedDownload(): Unit =
    mock
      .register(
        get(urlEqualTo(s"/$bucketKey"))
          .withHeader("Range", new EqualToPattern(s"bytes=$bytesRangeStart-$bytesRangeEnd"))
          .willReturn(
            aResponse()
              .withStatus(200)
              .withHeader("ETag", """"fba9dede5f27731c9771645a39863328"""")
              .withBody(rangeOfBody)
          )
      )

  def mockRangedDownloadSSE(): Unit =
    mock
      .register(
        get(urlEqualTo(s"/$bucketKey"))
          .withHeader("Range", new EqualToPattern(s"bytes=$bytesRangeStart-$bytesRangeEnd"))
          .withHeader("x-amz-server-side-encryption-customer-algorithm", new EqualToPattern("AES256"))
          .withHeader("x-amz-server-side-encryption-customer-key", new EqualToPattern(sseCustomerKey))
          .withHeader("x-amz-server-side-encryption-customer-key-MD5", new EqualToPattern(sseCustomerMd5Key))
          .willReturn(
            aResponse()
              .withStatus(200)
              .withHeader("ETag", """"fba9dede5f27731c9771645a39863328"""")
              .withBody(rangeOfBodySSE)
          )
      )

  def mockListBucket(): Unit =
    mock
      .register(
        get(urlEqualTo(s"/?list-type=2&prefix=$listPrefix")).willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/xml")
            .withBody(s"""<?xml version="1.0" encoding="UTF-8"?>
                        <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                            <Name>bucket</Name>
                            <Prefix>$listPrefix</Prefix>
                            <KeyCount>1</KeyCount>
                            <MaxKeys>1000</MaxKeys>
                            <IsTruncated>false</IsTruncated>
                            <Contents>
                                <Key>$listKey</Key>
                                <LastModified>2009-10-12T17:50:30.000Z</LastModified>
                                <ETag>&quot;fba9dede5f27731c9771645a39863328&quot;</ETag>
                                <Size>434234</Size>
                                <StorageClass>STANDARD</StorageClass>
                            </Contents>
                        </ListBucketResult>""".stripMargin)
        )
      )

  def mockUpload(): Unit = {
    mock
      .register(
        post(urlEqualTo(s"/$bucketKey?uploads")).willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("x-amz-id-2", "Uuag1LuByRx9e6j5Onimru9pO4ZVKnJ2Qz7/C1NPcfTWAtRPfTaOFg==")
            .withHeader("x-amz-request-id", "656c76696e6727732072657175657374")
            .withBody(s"""<?xml version="1.0" encoding="UTF-8"?>
                         |<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                         |  <Bucket>$bucket</Bucket>
                         |  <Key>$bucketKey</Key>
                         |  <UploadId>$uploadId</UploadId>
                         |</InitiateMultipartUploadResult>""".stripMargin)
        )
      )

    mock.register(
      put(urlEqualTo(s"/$bucketKey?partNumber=1&uploadId=$uploadId"))
        .withRequestBody(matching(body))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("x-amz-id-2", "Zn8bf8aEFQ+kBnGPBc/JaAf9SoWM68QDPS9+SyFwkIZOHUG2BiRLZi5oXw4cOCEt")
            .withHeader("x-amz-request-id", "5A37448A37622243")
            .withHeader("ETag", "\"" + etag + "\"")
        )
    )

    mock.register(
      post(urlEqualTo(s"/$bucketKey?uploadId=$uploadId"))
        .withRequestBody(containing("CompleteMultipartUpload"))
        .withRequestBody(containing(etag))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/xml; charset=UTF-8")
            .withHeader("x-amz-id-2", "Zn8bf8aEFQ+kBnGPBc/JaAf9SoWM68QDPS9+SyFwkIZOHUG2BiRLZi5oXw4cOCEt")
            .withHeader("x-amz-request-id", "5A37448A3762224333")
            .withBody(s"""<?xml version="1.0" encoding="UTF-8"?>
                         |<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                         |  <Location>$url</Location>
                         |  <Bucket>$bucket</Bucket>
                         |  <Key>$bucketKey</Key>
                         |  <ETag>"$etag"</ETag>
                         |</CompleteMultipartUploadResult>""".stripMargin)
        )
    )
  }

  def mockUploadSSE(): Unit = {
    mock
      .register(
        post(urlEqualTo(s"/$bucketKey?uploads"))
          .withHeader("x-amz-server-side-encryption-customer-algorithm", new EqualToPattern("AES256"))
          .withHeader("x-amz-server-side-encryption-customer-key", new EqualToPattern(sseCustomerKey))
          .withHeader("x-amz-server-side-encryption-customer-key-MD5", new EqualToPattern(sseCustomerMd5Key))
          .willReturn(
            aResponse()
              .withStatus(200)
              .withHeader("x-amz-id-2", "Uuag1LuByRx9e6j5Onimru9pO4ZVKnJ2Qz7/C1NPcfTWAtRPfTaOFg==")
              .withHeader("x-amz-request-id", "656c76696e6727732072657175657374")
              .withBody(s"""<?xml version="1.0" encoding="UTF-8"?>
                         |<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                         |  <Bucket>$bucket</Bucket>
                         |  <Key>$bucketKey</Key>
                         |  <UploadId>$uploadId</UploadId>
                         |</InitiateMultipartUploadResult>""".stripMargin)
          )
      )

    mock.register(
      put(urlEqualTo(s"/$bucketKey?partNumber=1&uploadId=$uploadId"))
        .withHeader("x-amz-server-side-encryption-customer-algorithm", new EqualToPattern("AES256"))
        .withHeader("x-amz-server-side-encryption-customer-key", new EqualToPattern(sseCustomerKey))
        .withHeader("x-amz-server-side-encryption-customer-key-MD5", new EqualToPattern(sseCustomerMd5Key))
        .withRequestBody(matching(body))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("x-amz-id-2", "Zn8bf8aEFQ+kBnGPBc/JaAf9SoWM68QDPS9+SyFwkIZOHUG2BiRLZi5oXw4cOCEt")
            .withHeader("x-amz-request-id", "5A37448A37622243")
            .withHeader("ETag", "\"" + etag + "\"")
        )
    )

    mock.register(
      post(urlEqualTo(s"/$bucketKey?uploadId=$uploadId"))
        .withRequestBody(containing("CompleteMultipartUpload"))
        .withRequestBody(containing(etag))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/xml; charset=UTF-8")
            .withHeader("x-amz-id-2", "Zn8bf8aEFQ+kBnGPBc/JaAf9SoWM68QDPS9+SyFwkIZOHUG2BiRLZi5oXw4cOCEt")
            .withHeader("x-amz-request-id", "5A37448A3762224333")
            .withBody(s"""<?xml version="1.0" encoding="UTF-8"?>
                         |<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                         |  <Location>$url</Location>
                         |  <Bucket>$bucket</Bucket>
                         |  <Key>$bucketKey</Key>
                         |  <ETag>"$etag"</ETag>
                         |</CompleteMultipartUploadResult>""".stripMargin)
        )
    )
  }
}

private object S3WireMockBase {

  def getCallerName(clazz: Class[_]): String = {
    val s = (Thread.currentThread.getStackTrace map (_.getClassName) drop 1)
      .dropWhile(_ matches "(java.lang.Thread|.*WireMockBase.?$)")
    val reduced = s.lastIndexWhere(_ == clazz.getName) match {
      case -1 => s
      case z => s drop (z + 1)
    }
    reduced.head.replaceFirst(""".*\.""", "").replaceAll("[^a-zA-Z_0-9]", "_")
  }

  def initServer(): WireMockServer = {
    val server = new WireMockServer(
      wireMockConfig()
        .dynamicPort()
        .dynamicHttpsPort()
        .keystorePath("./s3/src/test/resources/keystore.jks")
        .keystorePassword("abcdefg")
    )
    server.start()
    server
  }
}
