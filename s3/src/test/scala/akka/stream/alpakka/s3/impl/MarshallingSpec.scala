/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.s3.impl

import java.time.Instant

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{MediaTypes, _}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import akka.stream.alpakka.s3.{ListBucketResultCommonPrefixes, ListBucketResultContents}
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import akka.testkit.TestKit
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.BeforeAndAfterAll

import scala.collection.immutable.Seq
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

class MarshallingSpec(_system: ActorSystem)
    extends TestKit(_system)
    with AnyFlatSpecLike
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll
    with LogCapturing {

  def this() = this(ActorSystem("MarshallingSpec"))

  implicit val materializer = ActorMaterializer(ActorMaterializerSettings(system).withDebugLogging(true))
  implicit val ec = materializer.executionContext

  override protected def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  val xmlString = """<?xml version="1.0" encoding="UTF-8"?>
                    |<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                    |    <Name>bucket</Name>
                    |    <Prefix/>
                    |    <KeyCount>205</KeyCount>
                    |    <MaxKeys>1000</MaxKeys>
                    |    <IsTruncated>false</IsTruncated>
                    |    <Contents>
                    |        <Key>my-image.jpg</Key>
                    |        <LastModified>2009-10-12T17:50:30.000Z</LastModified>
                    |        <ETag>&quot;fba9dede5f27731c9771645a39863328&quot;</ETag>
                    |        <Size>434234</Size>
                    |        <StorageClass>STANDARD</StorageClass>
                    |    </Contents>
                    |    <Contents>
                    |        <Key>my-image2.jpg</Key>
                    |        <LastModified>2009-10-12T17:50:31.000Z</LastModified>
                    |        <ETag>&quot;599bab3ed2c697f1d26842727561fd94&quot;</ETag>
                    |        <Size>1234</Size>
                    |        <StorageClass>REDUCED_REDUNDANCY</StorageClass>
                    |    </Contents>
                    |    <CommonPrefixes>
                    |        <Prefix>prefix1/</Prefix>
                    |    </CommonPrefixes>
                    |    <CommonPrefixes>
                    |        <Prefix>prefix2/</Prefix>
                    |    </CommonPrefixes>
                    |</ListBucketResult>""".stripMargin

  it should "initiate multipart upload when the region is us-east-1" in {
    val entity = HttpEntity(MediaTypes.`application/xml` withCharset HttpCharsets.`UTF-8`, xmlString)

    val result = Marshalling.listBucketResultUnmarshaller(entity)

    result.futureValue shouldEqual ListBucketResult(
      false,
      None,
      Seq(
        ListBucketResultContents("bucket",
                                 "my-image.jpg",
                                 "fba9dede5f27731c9771645a39863328",
                                 434234,
                                 Instant.parse("2009-10-12T17:50:30Z"),
                                 "STANDARD"),
        ListBucketResultContents("bucket",
                                 "my-image2.jpg",
                                 "599bab3ed2c697f1d26842727561fd94",
                                 1234,
                                 Instant.parse("2009-10-12T17:50:31Z"),
                                 "REDUCED_REDUNDANCY")
      ),
      Seq(
        ListBucketResultCommonPrefixes("bucket", "prefix1/"),
        ListBucketResultCommonPrefixes("bucket", "prefix2/")
      )
    )
  }

  val listBucketV2TruncatedResponse = """<?xml version="1.0" encoding="UTF-8"?>
                                        |<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                                        |    <Name>bucket</Name>
                                        |    <Prefix/>
                                        |    <KeyCount>205</KeyCount>
                                        |    <MaxKeys>1000</MaxKeys>
                                        |    <NextContinuationToken>dummy/continuation/token</NextContinuationToken>
                                        |    <IsTruncated>true</IsTruncated>
                                        |    <Contents>
                                        |        <Key>my-image.jpg</Key>
                                        |        <LastModified>2009-10-12T17:50:30.000Z</LastModified>
                                        |        <ETag>&quot;fba9dede5f27731c9771645a39863328&quot;</ETag>
                                        |        <Size>434234</Size>
                                        |        <StorageClass>STANDARD</StorageClass>
                                        |    </Contents>
                                        |    <Contents>
                                        |        <Key>my-image2.jpg</Key>
                                        |        <LastModified>2009-10-12T17:50:31.000Z</LastModified>
                                        |        <ETag>&quot;599bab3ed2c697f1d26842727561fd94&quot;</ETag>
                                        |        <Size>1234</Size>
                                        |        <StorageClass>REDUCED_REDUNDANCY</StorageClass>
                                        |    </Contents>
                                        |</ListBucketResult>""".stripMargin
  it should "Use the value of the `NextContinuationToken` element as the continuation token of a truncated API V1 response" in {
    val entity =
      HttpEntity(MediaTypes.`application/xml` withCharset HttpCharsets.`UTF-8`, listBucketV2TruncatedResponse)

    val result = Marshalling.listBucketResultUnmarshaller(entity)

    result.futureValue shouldEqual ListBucketResult(
      true,
      Some("dummy/continuation/token"),
      Seq(
        ListBucketResultContents("bucket",
                                 "my-image.jpg",
                                 "fba9dede5f27731c9771645a39863328",
                                 434234,
                                 Instant.parse("2009-10-12T17:50:30Z"),
                                 "STANDARD"),
        ListBucketResultContents("bucket",
                                 "my-image2.jpg",
                                 "599bab3ed2c697f1d26842727561fd94",
                                 1234,
                                 Instant.parse("2009-10-12T17:50:31Z"),
                                 "REDUCED_REDUNDANCY")
      ),
      Nil
    )
  }

  val listBucketV1TruncatedResponse = """<?xml version="1.0" encoding="UTF-8"?>
                                        |<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                                        |    <Name>bucket</Name>
                                        |    <Prefix/>
                                        |    <KeyCount>205</KeyCount>
                                        |    <MaxKeys>1000</MaxKeys>
                                        |    <IsTruncated>true</IsTruncated>
                                        |    <Contents>
                                        |        <Key>my-image.jpg</Key>
                                        |        <LastModified>2009-10-12T17:50:30.000Z</LastModified>
                                        |        <ETag>&quot;fba9dede5f27731c9771645a39863328&quot;</ETag>
                                        |        <Size>434234</Size>
                                        |        <StorageClass>STANDARD</StorageClass>
                                        |    </Contents>
                                        |    <Contents>
                                        |        <Key>my-image2.jpg</Key>
                                        |        <LastModified>2009-10-12T17:50:31.000Z</LastModified>
                                        |        <ETag>&quot;599bab3ed2c697f1d26842727561fd94&quot;</ETag>
                                        |        <Size>1234</Size>
                                        |        <StorageClass>REDUCED_REDUNDANCY</StorageClass>
                                        |    </Contents>
                                        |</ListBucketResult>""".stripMargin
  it should "Use the value of the last `Key` element as the continuation token of a truncated API V1 response" in {
    val entity =
      HttpEntity(MediaTypes.`application/xml` withCharset HttpCharsets.`UTF-8`, listBucketV1TruncatedResponse)

    val result = Marshalling.listBucketResultUnmarshaller(entity)

    result.futureValue shouldEqual ListBucketResult(
      true,
      Some("my-image2.jpg"),
      Seq(
        ListBucketResultContents("bucket",
                                 "my-image.jpg",
                                 "fba9dede5f27731c9771645a39863328",
                                 434234,
                                 Instant.parse("2009-10-12T17:50:30Z"),
                                 "STANDARD"),
        ListBucketResultContents("bucket",
                                 "my-image2.jpg",
                                 "599bab3ed2c697f1d26842727561fd94",
                                 1234,
                                 Instant.parse("2009-10-12T17:50:31Z"),
                                 "REDUCED_REDUNDANCY")
      ),
      Nil
    )
  }

  it should "parse CopyPartResult" in {
    val xmlString =
      """
        |<CopyPartResult>
        | <ETag>"5b27a21a97fcf8a7004dd1d906e7a5ba"</ETag>
        | <LastModified>2009-10-28T22:32:00.000Z</LastModified>
        |</CopyPartResult>
      """.stripMargin

    val entity = HttpEntity(MediaTypes.`application/xml` withCharset HttpCharsets.`UTF-8`, xmlString)

    val result = Marshalling.copyPartResultUnmarshaller(entity)

    result.futureValue shouldEqual CopyPartResult(Instant.parse("2009-10-28T22:32:00.000Z"),
                                                  "5b27a21a97fcf8a7004dd1d906e7a5ba")
  }

}
