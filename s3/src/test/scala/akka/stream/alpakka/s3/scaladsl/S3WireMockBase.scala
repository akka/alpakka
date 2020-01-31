/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.s3.scaladsl

import akka.actor.ActorSystem
import akka.stream.alpakka.s3.S3Settings
import akka.stream.alpakka.s3.headers.ServerSideEncryption
import akka.stream.alpakka.s3.impl.S3Stream
import akka.stream.alpakka.s3.scaladsl.S3WireMockBase._
import akka.testkit.TestKit
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration._
import com.github.tomakehurst.wiremock.matching.{ContainsPattern, EqualToPattern}
import com.github.tomakehurst.wiremock.stubbing.Scenario
import com.typesafe.config.ConfigFactory
import software.amazon.awssdk.regions.Region

abstract class S3WireMockBase(_system: ActorSystem, val _wireMockServer: WireMockServer) extends TestKit(_system) {

  private def this(mock: WireMockServer) =
    this(ActorSystem(getCallerName(getClass), config(mock.port()).withFallback(ConfigFactory.load())), mock)

  def this() = {
    this(initServer())
    system.registerOnTermination(stopWireMockServer())
  }

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

  def mockFailureAfterInitiate(): Unit = {
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
            .withStatus(500)
        )
    )
  }

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
  val bucket = "test-bucket"
  val targetBucket = "test-target-bucket"
  val targetBucketKey = "testTargetKey"
  val uploadId = "VXBsb2FkIElEIGZvciA2aWWpbmcncyBteS1tb3ZpZS5tMnRzIHVwbG9hZA"
  val etag = "5b27a21a97fcf8a7004dd1d906e7a5ba"
  val etagSSE = "5b27a21a97fcf8a7004dd1d906e7a5cd"
  val url = s"http://testbucket.s3.amazonaws.com/testKey"
  val targetUrl = s"http://$targetBucket.s3.amazonaws.com/$targetBucketKey"
  val (bytesRangeStart, bytesRangeEnd) = (2, 10)
  val rangeOfBody = body.getBytes.slice(bytesRangeStart, bytesRangeEnd + 1)
  val rangeOfBodySSE = bodySSE.getBytes.slice(bytesRangeStart, bytesRangeEnd + 1)
  val listPrefix = "testPrefix"
  val listDelimiter = "/"
  val listCommonPrefix = "commonPrefix/"
  val listKey = "testingKey.txt"

  val sseCustomerKey = "key"
  val sseCustomerMd5Key = "md5"
  val sseCustomerKeys = ServerSideEncryption.customerKeys(sseCustomerKey).withMd5(sseCustomerMd5Key)

  def mockDownload(): Unit =
    mock
      .register(
        get(urlEqualTo(s"/$bucketKey")).willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("ETag", """"fba9dede5f27731c9771645a39863328"""")
            .withHeader("Content-Length", body.length.toString)
            .withBody(body)
        )
      )

  def mockDownload(region: Region): Unit =
    mock
      .register(
        get(urlEqualTo(s"/$bucketKey"))
          .withHeader("Authorization", new ContainsPattern(region.id))
          .willReturn(
            aResponse()
              .withStatus(200)
              .withHeader("ETag", """"fba9dede5f27731c9771645a39863328"""")
              .withHeader("Content-Length", body.length.toString)
              .withBody(body)
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

  def mockDownloadSSECWithVersion(versionId: String): Unit =
    mock
      .register(
        get(urlEqualTo(s"/$bucketKey?versionId=$versionId"))
          .withHeader("x-amz-server-side-encryption-customer-algorithm", new EqualToPattern("AES256"))
          .withHeader("x-amz-server-side-encryption-customer-key", new EqualToPattern(sseCustomerKey))
          .withHeader("x-amz-server-side-encryption-customer-key-MD5", new EqualToPattern(sseCustomerMd5Key))
          .willReturn(
            aResponse()
              .withStatus(200)
              .withHeader("ETag", """"fba9dede5f27731c9771645a39863328"""")
              .withHeader("x-amz-version-id", versionId)
              .withBody(bodySSE)
          )
      )

  def mockHead(contentLength: Long): Unit =
    mock
      .register(
        head(urlEqualTo(s"/$bucketKey")).willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("ETag", s""""$etag"""")
            .withHeader("Content-Length", contentLength.toString)
        )
      )

  def mockHeadWithVersion(versionId: String): Unit =
    mock
      .register(
        head(urlEqualTo(s"/$bucketKey?versionId=$versionId")).willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("ETag", s""""$etag"""")
            .withHeader("Content-Length", "8")
            .withHeader("x-amz-version-id", versionId)
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
            aResponse().withStatus(200).withHeader("ETag", s""""$etagSSE"""").withHeader("Content-Length", "8")
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
            .withBody(s"""|<?xml version="1.0" encoding="UTF-8"?>
                        |<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                        |    <Name>bucket</Name>
                        |    <Prefix>$listPrefix</Prefix>
                        |    <KeyCount>1</KeyCount>
                        |    <MaxKeys>1000</MaxKeys>
                        |    <IsTruncated>false</IsTruncated>
                        |    <Contents>
                        |        <Key>$listKey</Key>
                        |        <LastModified>2009-10-12T17:50:30.000Z</LastModified>
                        |        <ETag>&quot;fba9dede5f27731c9771645a39863328&quot;</ETag>
                        |        <Size>434234</Size>
                        |        <StorageClass>STANDARD</StorageClass>
                        |    </Contents>
                        |</ListBucketResult>""".stripMargin)
        )
      )

  def mockListBucketVersion1(): Unit =
    mock
      .register(
        get(urlEqualTo(s"/?prefix=$listPrefix")).willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/xml")
            .withBody(s"""|<?xml version="1.0" encoding="UTF-8"?>
                        |<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                        |    <Name>bucket</Name>
                        |    <Prefix>$listPrefix</Prefix>
                        |    <Marker/>
                        |    <MaxKeys>1000</MaxKeys>
                        |    <IsTruncated>false</IsTruncated>
                        |    <Contents>
                        |        <Key>$listKey</Key>
                        |        <LastModified>2009-10-12T17:50:30.000Z</LastModified>
                        |        <ETag>&quot;fba9dede5f27731c9771645a39863328&quot;</ETag>
                        |        <Size>434234</Size>
                        |        <StorageClass>STANDARD</StorageClass>
                        |    </Contents>
                        |</ListBucketResult>""".stripMargin)
        )
      )

  def mockListBucketAndCommonPrefixes(): Unit =
    mock
      .register(
        get(urlEqualTo(s"/?list-type=2&prefix=$listPrefix&delimiter=$listDelimiter")).willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/xml")
            .withBody(s"""|<?xml version="1.0" encoding="UTF-8"?>
                          |<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                          |    <Name>bucket</Name>
                          |    <Prefix>$listPrefix</Prefix>
                          |    <KeyCount>1</KeyCount>
                          |    <MaxKeys>1000</MaxKeys>
                          |    <IsTruncated>false</IsTruncated>
                          |    <Contents>
                          |        <Key>$listKey</Key>
                          |        <LastModified>2009-10-12T17:50:30.000Z</LastModified>
                          |        <ETag>&quot;fba9dede5f27731c9771645a39863328&quot;</ETag>
                          |        <Size>434234</Size>
                          |        <StorageClass>STANDARD</StorageClass>
                          |    </Contents>
                          |    <CommonPrefixes>
                          |        <Prefix>$listCommonPrefix</Prefix>
                          |    </CommonPrefixes>
                          |</ListBucketResult>""".stripMargin)
        )
      )

  def mockListBucketAndCommonPrefixesVersion1(): Unit =
    mock
      .register(
        get(urlEqualTo(s"/?prefix=$listPrefix&delimiter=$listDelimiter")).willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/xml")
            .withBody(s"""|<?xml version="1.0" encoding="UTF-8"?>
                          |<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                          |    <Name>bucket</Name>
                          |    <Prefix>$listPrefix</Prefix>
                          |    <Marker/>
                          |    <MaxKeys>1000</MaxKeys>
                          |    <IsTruncated>false</IsTruncated>
                          |    <Contents>
                          |        <Key>$listKey</Key>
                          |        <LastModified>2009-10-12T17:50:30.000Z</LastModified>
                          |        <ETag>&quot;fba9dede5f27731c9771645a39863328&quot;</ETag>
                          |        <Size>434234</Size>
                          |        <StorageClass>STANDARD</StorageClass>
                          |    </Contents>
                          |    <CommonPrefixes>
                          |        <Prefix>$listCommonPrefix</Prefix>
                          |    </CommonPrefixes>
                          |</ListBucketResult>""".stripMargin)
        )
      )

  def mockUpload(): Unit = mockUpload(body)
  def mockUpload(expectedBody: String): Unit = {
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
        .withRequestBody(if (expectedBody.isEmpty) absent() else matching(expectedBody))
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

  def mockUploadWithInternalError(expectedBody: String): Unit = {
    mock
      .register(
        post(urlEqualTo(s"/$bucketKey?uploads"))
          .inScenario("InternalError")
          .whenScenarioStateIs(Scenario.STARTED)
          .willReturn(
            aResponse()
              .withStatus(500)
              .withHeader("x-amz-id-2", "Uuag1LuByRx9e6j5Onimru9pO4ZVKnJ2Qz7/C1NPcfTWAtRPfTaOFg==")
              .withHeader("x-amz-request-id", "656c76696e6727732072657175657374")
              .withBody(s"""<?xml version="1.0" encoding="UTF-8"?>
                         |<Error>
                         |  <Code>InternalError</Code>
                         |  <Message>We encountered an internal error. Please try again.</Message>
                         |  <Resource>$bucket/$bucketKey</Resource>
                         |  <RequestId>4442587FB7D0A2F9</RequestId>
                         |</Error>""".stripMargin)
          )
          .willSetStateTo("Recover")
      )
    mock
      .register(
        post(urlEqualTo(s"/$bucketKey?uploads"))
          .inScenario("InternalError")
          .whenScenarioStateIs("Recover")
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
        .withRequestBody(matching(expectedBody))
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

  def mockCopy(): Unit = mockCopy(body.length)
  def mockCopyMinChunkSize(): Unit = mockCopy(S3Stream.MinChunkSize)
  def mockCopy(expectedContentLength: Int): Unit = {
    mock.register(
      head(urlEqualTo(s"/$bucketKey"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("x-amz-id-2", "ef8yU9AS1ed4OpIszj7UDNEHGran")
            .withHeader("x-amz-request-id", "318BC8BC143432E5")
            .withHeader("ETag", "\"" + etag + "\"")
            .withHeader("Content-Length", s"$expectedContentLength")
        )
    )

    mock
      .register(
        post(urlEqualTo(s"/$targetBucketKey?uploads")).willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("x-amz-id-2", "Uuag1LuByRx9e6j5Onimru9pO4ZVKnJ2Qz7/C1NPcfTWAtRPfTaOFg==")
            .withHeader("x-amz-request-id", "656c76696e6727732072657175657374")
            .withBody(s"""<?xml version="1.0" encoding="UTF-8"?>
                         |<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                         |  <Bucket>$targetBucket</Bucket>
                         |  <Key>$targetBucketKey</Key>
                         |  <UploadId>$uploadId</UploadId>
                         |</InitiateMultipartUploadResult>""".stripMargin)
        )
      )

    mock.register(
      put(urlEqualTo(s"/$targetBucketKey?partNumber=1&uploadId=$uploadId"))
        .withHeader("x-amz-copy-source", new EqualToPattern(s"/$bucket/$bucketKey"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("x-amz-id-2", "Zn8bf8aEFQ+kBnGPBc/JaAf9SoWM68QDPS9+SyFwkIZOHUG2BiRLZi5oXw4cOCEt")
            .withHeader("x-amz-request-id", "5A37448A37622243")
            .withBody(s"""<?xml version="1.0" encoding="UTF-8"?>
                 |<CopyPartResult>
                 |  <ETag>"$etag"</ETag>
                 |  <LastModified>2009-10-28T22:32:00.000Z</LastModified>
                 |</CopyPartResult>
               """.stripMargin)
        )
    )

    mock.register(
      post(urlEqualTo(s"/$targetBucketKey?uploadId=$uploadId"))
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
                         |  <Location>$targetUrl</Location>
                         |  <Bucket>$targetBucket</Bucket>
                         |  <Key>$targetBucketKey</Key>
                         |  <ETag>"$etag"</ETag>
                         |</CompleteMultipartUploadResult>""".stripMargin)
        )
    )
  }

  def mockCopyVersioned(): Unit = mockCopyVersioned(body.length)
  def mockCopyVersioned(expectedContentLength: Int): Unit = {
    mock.register(
      head(urlEqualTo(s"/$bucketKey?versionId=3/L4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY+MTRCxf3vjVBH40Nr8X8gdRQBpUMLUo"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("x-amz-id-2", "ef8yU9AS1ed4OpIszj7UDNEHGran")
            .withHeader("x-amz-request-id", "318BC8BC143432E5")
            .withHeader("x-amz-version-id", "3/L4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY+MTRCxf3vjVBH40Nr8X8gdRQBpUMLUo")
            .withHeader("ETag", "\"" + etag + "\"")
            .withHeader("Content-Length", s"$expectedContentLength")
        )
    )

    mock
      .register(
        post(urlEqualTo(s"/$targetBucketKey?uploads")).willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("x-amz-id-2", "Uuag1LuByRx9e6j5Onimru9pO4ZVKnJ2Qz7/C1NPcfTWAtRPfTaOFg==")
            .withHeader("x-amz-request-id", "656c76696e6727732072657175657374")
            .withBody(s"""<?xml version="1.0" encoding="UTF-8"?>
                         |<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                         |  <Bucket>$targetBucket</Bucket>
                         |  <Key>$targetBucketKey</Key>
                         |  <UploadId>$uploadId</UploadId>
                         |</InitiateMultipartUploadResult>""".stripMargin)
        )
      )

    mock.register(
      put(urlEqualTo(s"/$targetBucketKey?partNumber=1&uploadId=$uploadId"))
        .withHeader("x-amz-copy-source",
                    new EqualToPattern(
                      s"/$bucket/$bucketKey?versionId=3/L4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY+MTRCxf3vjVBH40Nr8X8gdRQBpUMLUo"
                    ))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("x-amz-id-2", "Zn8bf8aEFQ+kBnGPBc/JaAf9SoWM68QDPS9+SyFwkIZOHUG2BiRLZi5oXw4cOCEt")
            .withHeader("x-amz-request-id", "5A37448A37622243")
            .withHeader("x-amz-copy-source-version-id",
                        "3/L4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY+MTRCxf3vjVBH40Nr8X8gdRQBpUMLUo")
            .withBody(s"""<?xml version="1.0" encoding="UTF-8"?>
                         |<CopyPartResult>
                         |  <ETag>"$etag"</ETag>
                         |  <LastModified>2009-10-28T22:32:00.000Z</LastModified>
                         |</CopyPartResult>
               """.stripMargin)
        )
    )

    mock.register(
      post(urlEqualTo(s"/$targetBucketKey?uploadId=$uploadId"))
        .withRequestBody(containing("CompleteMultipartUpload"))
        .withRequestBody(containing(etag))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/xml; charset=UTF-8")
            .withHeader("x-amz-id-2", "Zn8bf8aEFQ+kBnGPBc/JaAf9SoWM68QDPS9+SyFwkIZOHUG2BiRLZi5oXw4cOCEt")
            .withHeader("x-amz-request-id", "5A37448A3762224333")
            .withHeader("x-amz-version-id", "43jfkodU8493jnFJD9fjj3HHNVfdsQUIFDNsidf038jfdsjGFDSIRp")
            .withBody(s"""<?xml version="1.0" encoding="UTF-8"?>
                         |<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                         |  <Location>$targetUrl</Location>
                         |  <Bucket>$targetBucket</Bucket>
                         |  <Key>$targetBucketKey</Key>
                         |  <ETag>"$etag"</ETag>
                         |</CompleteMultipartUploadResult>""".stripMargin)
        )
    )
  }

  def mockCopySSE(): Unit = {
    val expectedContentLength = bodySSE.length
    mock.register(
      head(urlEqualTo(s"/$bucketKey"))
        .withHeader("x-amz-server-side-encryption-customer-algorithm", new EqualToPattern("AES256"))
        .withHeader("x-amz-server-side-encryption-customer-key", new EqualToPattern(sseCustomerKey))
        .withHeader("x-amz-server-side-encryption-customer-key-MD5", new EqualToPattern(sseCustomerMd5Key))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("x-amz-id-2", "ef8yU9AS1ed4OpIszj7UDNEHGran")
            .withHeader("x-amz-request-id", "318BC8BC143432E5")
            .withHeader("ETag", "\"" + etag + "\"")
            .withHeader("Content-Length", s"$expectedContentLength")
        )
    )

    mock
      .register(
        post(urlEqualTo(s"/$targetBucketKey?uploads"))
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
                           |  <Bucket>$targetBucket</Bucket>
                           |  <Key>$targetBucketKey</Key>
                           |  <UploadId>$uploadId</UploadId>
                           |</InitiateMultipartUploadResult>""".stripMargin)
          )
      )

    mock.register(
      put(urlEqualTo(s"/$targetBucketKey?partNumber=1&uploadId=$uploadId"))
        .withHeader("x-amz-copy-source", new EqualToPattern(s"/$bucket/$bucketKey"))
        .withHeader("x-amz-server-side-encryption-customer-algorithm", new EqualToPattern("AES256"))
        .withHeader("x-amz-server-side-encryption-customer-key", new EqualToPattern(sseCustomerKey))
        .withHeader("x-amz-server-side-encryption-customer-key-MD5", new EqualToPattern(sseCustomerMd5Key))
        .withHeader("x-amz-copy-source-server-side-encryption-customer-algorithm", new EqualToPattern("AES256"))
        .withHeader("x-amz-copy-source-server-side-encryption-customer-key", new EqualToPattern(sseCustomerKey))
        .withHeader("x-amz-copy-source-server-side-encryption-customer-key-MD5", new EqualToPattern(sseCustomerMd5Key))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("x-amz-id-2", "Zn8bf8aEFQ+kBnGPBc/JaAf9SoWM68QDPS9+SyFwkIZOHUG2BiRLZi5oXw4cOCEt")
            .withHeader("x-amz-request-id", "5A37448A37622243")
            .withBody(s"""<?xml version="1.0" encoding="UTF-8"?>
                 |<CopyPartResult>
                 |  <ETag>"$etag"</ETag>
                 |  <LastModified>2009-10-28T22:32:00.000Z</LastModified>
                 |</CopyPartResult>
               """.stripMargin)
        )
    )

    mock.register(
      post(urlEqualTo(s"/$targetBucketKey?uploadId=$uploadId"))
        .withRequestBody(containing("CompleteMultipartUpload"))
        .withRequestBody(containing(etag))
        .withHeader("x-amz-server-side-encryption-customer-algorithm", new EqualToPattern("AES256"))
        .withHeader("x-amz-server-side-encryption-customer-key", new EqualToPattern(sseCustomerKey))
        .withHeader("x-amz-server-side-encryption-customer-key-MD5", new EqualToPattern(sseCustomerMd5Key))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/xml; charset=UTF-8")
            .withHeader("x-amz-id-2", "Zn8bf8aEFQ+kBnGPBc/JaAf9SoWM68QDPS9+SyFwkIZOHUG2BiRLZi5oXw4cOCEt")
            .withHeader("x-amz-request-id", "5A37448A3762224333")
            .withBody(s"""<?xml version="1.0" encoding="UTF-8"?>
                         |<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                         |  <Location>$targetUrl</Location>
                         |  <Bucket>$targetBucket</Bucket>
                         |  <Key>$targetBucketKey</Key>
                         |  <ETag>"$etag"</ETag>
                         |</CompleteMultipartUploadResult>""".stripMargin)
        )
    )
  }

  def mockCopyMulti(): Unit = {
    val expectedContentLength = (5242880 * 1.5).toInt
    mock.register(
      head(urlEqualTo(s"/$bucketKey"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("x-amz-id-2", "ef8yU9AS1ed4OpIszj7UDNEHGran")
            .withHeader("x-amz-request-id", "318BC8BC143432E5")
            .withHeader("ETag", "\"" + etag + "\"")
            .withHeader("Content-Length", s"$expectedContentLength")
        )
    )

    mock
      .register(
        post(urlEqualTo(s"/$targetBucketKey?uploads")).willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("x-amz-id-2", "Uuag1LuByRx9e6j5Onimru9pO4ZVKnJ2Qz7/C1NPcfTWAtRPfTaOFg==")
            .withHeader("x-amz-request-id", "656c76696e6727732072657175657374")
            .withBody(s"""<?xml version="1.0" encoding="UTF-8"?>
                         |<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                         |  <Bucket>$targetBucket</Bucket>
                         |  <Key>$targetBucketKey</Key>
                         |  <UploadId>$uploadId</UploadId>
                         |</InitiateMultipartUploadResult>""".stripMargin)
        )
      )

    mock.register(
      put(urlEqualTo(s"/$targetBucketKey?partNumber=1&uploadId=$uploadId"))
        .withHeader("x-amz-copy-source", new EqualToPattern(s"/$bucket/$bucketKey"))
        .withHeader("x-amz-copy-source-range", new EqualToPattern("bytes=0-5242879"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("x-amz-id-2", "Zn8bf8aEFQ+kBnGPBc/JaAf9SoWM68QDPS9+SyFwkIZOHUG2BiRLZi5oXw4cOCEt")
            .withHeader("x-amz-request-id", "5A37448A37622243")
            .withBody(s"""<?xml version="1.0" encoding="UTF-8"?>
                 |<CopyPartResult>
                 |  <ETag>"$etag"</ETag>
                 |  <LastModified>2009-10-28T22:32:00.000Z</LastModified>
                 |</CopyPartResult>
               """.stripMargin)
        )
    )

    mock.register(
      put(urlEqualTo(s"/$targetBucketKey?partNumber=2&uploadId=$uploadId"))
        .withHeader("x-amz-copy-source", new EqualToPattern(s"/$bucket/$bucketKey"))
        .withHeader("x-amz-copy-source-range", new EqualToPattern("bytes=5242880-7864319"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("x-amz-id-2", "Zn8bf8aEFQ+kBnGPBc/JaAf9SoWM68QDPS9+SyFwkIZOHUG2BiRLZi5oXw4cOCEt")
            .withHeader("x-amz-request-id", "5A37448A37622243")
            .withBody(s"""<?xml version="1.0" encoding="UTF-8"?>
                 |<CopyPartResult>
                 |  <ETag>"$etag"</ETag>
                 |  <LastModified>2009-10-28T22:32:00.000Z</LastModified>
                 |</CopyPartResult>
               """.stripMargin)
        )
    )

    mock.register(
      post(urlEqualTo(s"/$targetBucketKey?uploadId=$uploadId"))
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
                         |  <Location>$targetUrl</Location>
                         |  <Bucket>$targetBucket</Bucket>
                         |  <Key>$targetBucketKey</Key>
                         |  <ETag>"$etag"</ETag>
                         |</CompleteMultipartUploadResult>""".stripMargin)
        )
    )
  }

  def mockMakingBucket(): Unit =
    mock.register(
      put(urlEqualTo("/")).willReturn(
        aResponse()
          .withStatus(200)
      )
    )

  def mockDeletingBucket(): Unit = mock.register(
    delete(urlEqualTo("/")).willReturn(
      aResponse()
        .withStatus(200)
    )
  )

  def mockCheckingBucketStateForNonExistingBucket(): Unit =
    mock.register(
      head(urlEqualTo("/")).willReturn(aResponse().withStatus(404))
    )

  def mockCheckingBucketStateForExistingBucket(): Unit =
    mock.register(
      head(urlEqualTo("/")).willReturn(aResponse().withStatus(200))
    )

  def mockCheckingBucketStateForBucketWithoutRights(): Unit =
    mock.register(
      head(urlEqualTo("/")).willReturn(aResponse().withStatus(403))
    )
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

  private def config(proxyPort: Int) = ConfigFactory.parseString(s"""
    |${S3Settings.ConfigPath} {
    |  aws {
    |    credentials {
    |      provider = static
    |      access-key-id = my-AWS-access-key-ID
    |      secret-access-key = my-AWS-password
    |    }
    |    region {
    |      provider = static
    |      default-region = "us-east-1"
    |    }
    |  }
    |  path-style-access = false
    |  endpoint-url = "http://localhost:$proxyPort"
    |}
    """.stripMargin)
}
