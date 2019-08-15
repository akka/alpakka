/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.storage.scaladsl

import akka.actor.ActorSystem
import akka.stream.alpakka.googlecloud.storage.GCStorageSettings
import akka.stream.alpakka.googlecloud.storage.scaladsl.GCStorageWiremockBase._
import akka.testkit.TestKit
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock
import com.github.tomakehurst.wiremock.client.WireMock.{aResponse, matching, urlEqualTo}
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import com.typesafe.config.ConfigFactory

import scala.util.Random

abstract class GCStorageWiremockBase(_system: ActorSystem, _wireMockServer: WireMockServer) extends TestKit(_system) {

  def this(mock: WireMockServer) =
    this(ActorSystem(getCallerName(getClass), config(mock.port()).withFallback(ConfigFactory.load())), mock)
  def this() = this(initServer())

  val port = _wireMockServer.port()
  val mock = new WireMock("localhost", port)

  def stopWireMockServer(): Unit = _wireMockServer.stop()

  val bucketName = "alpakka"
  val fileName = "file1.txt"

  val storageObjectJson =
    s"""
       |{
       |  "etag":"CMDm8oLo7N4CEAE=",
       |  "name":"$fileName",
       |  "size":"5",
       |  "generation":"1543055053992768",
       |  "crc32c":"AtvFhg==",
       |  "md5Hash":"emjwm9mSZxuzsZpecLeCfg==",
       |  "timeCreated":"2018-11-24T10:24:13.992Z",
       |  "selfLink":"https://www.googleapis.com/storage/v1/b/alpakka/o/$fileName",
       |  "timeStorageClassUpdated":"2018-11-24T10:24:13.992Z",
       |  "storageClass":"MULTI_REGIONAL",
       |  "id":"alpakka/GoogleCloudStorageClientIntegrationSpec63f9feb6-e800-472b-a51f-48e1c6d5d43f/testa.txt/1543055053992768",
       |  "contentType":"text/plain; charset=UTF-8",
       |  "updated":"2018-11-24T10:24:13.992Z",
       |  "mediaLink":"https://www.googleapis.com/download/storage/v1/b/alpakka/o/$fileName?generation=1543055053992768&alt=media",
       |  "bucket":"alpakka",
       |  "kind":"storage#object",
       |  "metageneration":"1"
       |}""".stripMargin

  def getRandomString(size: Int): String =
    Random.alphanumeric.take(size).mkString

  def mockTokenApi(): Unit =
    mock.register(
      WireMock
        .post(
          urlEqualTo("/oauth2/v4/token")
        )
        .willReturn(
          aResponse()
            .withStatus(200)
            .withBody(
              s"""{"access_token": "${TestCredentials.accessToken}", "token_type": "String", "expires_in": 3600}"""
            )
            .withHeader("Content-Type", "application/json")
        )
    )

  def mockBucketCreate(location: String): Unit = {
    val createBucketJsonRequest = s"""{"name":"$bucketName","location":"$location"}"""
    val createBucketJsonResponse =
      s"""
         |{
         |  "etag":"CAE=",
         |  "name":"$bucketName",
         |  "location":"EUROPE-WEST1",
         |  "timeCreated":"2018-11-24T06:51:56.529Z",
         |  "selfLink":"https://www.googleapis.com/storage/v1/b/alpakka_7fb9d77f-d327-42db-b0d6-538db2a1a3ae",
         |  "storageClass":"STANDARD",
         |  "id":"alpakka_7fb9d77f-d327-42db-b0d6-538db2a1a3ae",
         |  "updated":"2018-11-24T06:51:56.529Z",
         |  "projectNumber":"250058024243",
         |  "iamConfiguration":{"bucketPolicyOnly":{"enabled":false}},
         |  "kind":"storage#bucket",
         |  "metageneration":"1"
         |}""".stripMargin

    mock.register(
      WireMock
        .post(
          urlEqualTo(s"/b?project=testX-XXXXX")
        )
        .withRequestBody(WireMock.equalToJson(createBucketJsonRequest))
        .withHeader("Authorization", WireMock.equalTo("Bearer " + TestCredentials.accessToken))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withBody(createBucketJsonResponse)
            .withHeader("Content-Type", "application/json")
        )
    )
  }

  def mockBucketCreateFailure(location: String): Unit = {
    val createBucketJsonRequest = s"""{"name":"$bucketName","location":"$location"}"""
    mock.register(
      WireMock
        .post(
          urlEqualTo(s"/b?project=testX-XXXXX")
        )
        .withRequestBody(WireMock.equalToJson(createBucketJsonRequest))
        .withHeader("Authorization", WireMock.equalTo("Bearer " + TestCredentials.accessToken))
        .willReturn(
          aResponse()
            .withStatus(400)
            .withBody("Create failed")
        )
    )
  }

  def mockDeleteBucket(): Unit = {
    val deleteBucketJsonResponse = """{"kind":"storage#objects"}"""

    mock.register(
      WireMock
        .delete(
          urlEqualTo(s"/b/$bucketName")
        )
        .withHeader("Authorization", WireMock.equalTo("Bearer " + TestCredentials.accessToken))
        .willReturn(
          aResponse()
            .withStatus(204)
            .withBody(deleteBucketJsonResponse)
            .withHeader("Content-Type", "application/json")
        )
    )
  }

  def mockDeleteBucketFailure(): Unit =
    mock.register(
      WireMock
        .delete(
          urlEqualTo(s"/b/$bucketName")
        )
        .withHeader("Authorization", WireMock.equalTo("Bearer " + TestCredentials.accessToken))
        .willReturn(
          aResponse()
            .withStatus(400)
            .withBody("Delete failed")
            .withHeader("Content-Type", "application/json")
        )
    )

  def mockGetExistingBucket(): Unit = {
    val getBucketJsonResponse =
      s"""
        |{
        |  "etag":"CAE=",
        |  "name":"$bucketName",
        |  "location":"EUROPE-WEST1",
        |  "timeCreated":"2018-11-24T06:51:56.529Z",
        |  "selfLink":"https://www.googleapis.com/storage/v1/b/alpakka_7fb9d77f-d327-42db-b0d6-538db2a1a3ae",
        |  "storageClass":"STANDARD",
        |  "id":"alpakka_7fb9d77f-d327-42db-b0d6-538db2a1a3ae",
        |  "updated":"2018-11-24T06:51:56.529Z",
        |  "projectNumber":"250058024243",
        |  "iamConfiguration":{"bucketPolicyOnly":{"enabled":false}},
        |  "kind":"storage#bucket",
        |  "metageneration":"1"
        |}""".stripMargin

    mock.register(
      WireMock
        .get(
          urlEqualTo(s"/b/$bucketName")
        )
        .withHeader("Authorization", WireMock.equalTo("Bearer " + TestCredentials.accessToken))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withBody(getBucketJsonResponse)
            .withHeader("Content-Type", "application/json")
        )
    )
  }

  def mockGetNonExistingBucket(): Unit =
    mock.register(
      WireMock
        .get(
          urlEqualTo(s"/b/$bucketName")
        )
        .withHeader("Authorization", WireMock.equalTo("Bearer " + TestCredentials.accessToken))
        .willReturn(
          aResponse()
            .withStatus(404)
            .withHeader("Content-Type", "application/json")
        )
    )

  def mockGetBucketFailure(): Unit =
    mock.register(
      WireMock
        .get(
          urlEqualTo(s"/b/$bucketName")
        )
        .withHeader("Authorization", WireMock.equalTo("Bearer " + TestCredentials.accessToken))
        .willReturn(
          aResponse()
            .withStatus(400)
            .withBody("Get bucket failed")
            .withHeader("Content-Type", "application/json")
        )
    )

  def mockEmptyBucketListing(): Unit = {
    val emptyBucketJsonResponse = """{"kind":"storage#objects"}"""

    mock.register(
      WireMock
        .get(
          urlEqualTo(s"/b/$bucketName/o")
        )
        .withHeader("Authorization", WireMock.equalTo("Bearer " + TestCredentials.accessToken))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withBody(emptyBucketJsonResponse)
            .withHeader("Content-Type", "application/json")
        )
    )
  }

  def mockNonExistingFolderListing(folder: String): Unit = {
    val emptyFolderJsonResponse = """{"kind":"storage#objects"}"""

    mock.register(
      WireMock
        .get(
          urlEqualTo(s"/b/$bucketName/o?prefix=$folder")
        )
        .withHeader("Authorization", WireMock.equalTo("Bearer " + TestCredentials.accessToken))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withBody(emptyFolderJsonResponse)
            .withHeader("Content-Type", "application/json")
        )
    )
  }

  def mockDeleteObject(name: String): Unit =
    mock.register(
      WireMock
        .delete(
          urlEqualTo(s"/b/$bucketName/o/$name")
        )
        .withHeader("Authorization", WireMock.equalTo("Bearer " + TestCredentials.accessToken))
        .willReturn(
          aResponse()
            .withStatus(204)
        )
    )

  def mockNonExistingDeleteObject(name: String): Unit =
    mock.register(
      WireMock
        .delete(
          urlEqualTo(s"/b/$bucketName/o/$name")
        )
        .withHeader("Authorization", WireMock.equalTo("Bearer " + TestCredentials.accessToken))
        .willReturn(
          aResponse()
            .withStatus(404)
        )
    )

  def mockDeleteObjectFailure(name: String): Unit =
    mock.register(
      WireMock
        .delete(
          urlEqualTo(s"/b/$bucketName/o/$name")
        )
        .withHeader("Authorization", WireMock.equalTo("Bearer " + TestCredentials.accessToken))
        .willReturn(
          aResponse()
            .withStatus(400)
            .withBody("Delete object failed")
        )
    )

  def mockObjectExists(name: String): Unit =
    mock.register(
      WireMock
        .head(
          urlEqualTo(s"/b/$bucketName/o/$name")
        )
        .withHeader("Authorization", WireMock.equalTo("Bearer " + TestCredentials.accessToken))
        .willReturn(
          aResponse()
            .withStatus(200)
        )
    )

  def mockObjectDoesNotExist(name: String): Unit =
    mock.register(
      WireMock
        .head(
          urlEqualTo(s"/b/$bucketName/o/$name")
        )
        .withHeader("Authorization", WireMock.equalTo("Bearer " + TestCredentials.accessToken))
        .willReturn(
          aResponse()
            .withStatus(404)
        )
    )

  def mockObjectExistsFailure(name: String): Unit =
    mock.register(
      WireMock
        .head(
          urlEqualTo(s"/b/$bucketName/o/$name")
        )
        .withHeader("Authorization", WireMock.equalTo("Bearer " + TestCredentials.accessToken))
        .willReturn(
          aResponse()
            .withStatus(400)
        )
    )

  def mockNonExistingBucketListing(folder: Option[String] = None): Unit =
    mock.register(
      WireMock
        .get(
          urlEqualTo(s"/b/$bucketName/o" + folder.map(f => s"?prefix=$f").getOrElse(""))
        )
        .withHeader("Authorization", WireMock.equalTo("Bearer " + TestCredentials.accessToken))
        .willReturn(
          aResponse()
            .withStatus(404)
        )
    )

  def mockNonExistingBucketListingJava(folder: String): Unit =
    mockNonExistingBucketListing(Some(folder))

  def mockNonExistingBucketListingJava(): Unit =
    mockNonExistingBucketListing(None)

  def mockBucketListingJava(firstFileName: String, secondFileName: String): Unit =
    mockBucketListing(firstFileName, secondFileName)

  def mockBucketListingJava(firstFileName: String, secondFileName: String, folder: String): Unit =
    mockBucketListing(firstFileName, secondFileName, Some(folder))

  def mockBucketListing(firstFileName: String, secondFileName: String, folder: Option[String] = None): Unit = {
    val nextPageToken = "CiAyMDA1MDEwMy8wMDAwOTUwMTQyLTA1LTAwMDAwNi5uYw"

    val listJsonItemsPageOne =
      s"""{
         |  "kind":"storage#objects",
         |  "nextPageToken": "$nextPageToken",
         |  "items":[
         |    {
         |      "etag":"CMDm8oLo7N4CEAE=",
         |      "name":"$firstFileName",
         |      "size":"5",
         |      "generation":"1543055053992768",
         |      "crc32c":"AtvFhg==",
         |      "md5Hash":"emjwm9mSZxuzsZpecLeCfg==",
         |      "timeCreated":"2018-11-24T10:24:13.992Z",
         |      "selfLink":"https://www.googleapis.com/storage/v1/b/alpakka/o/$firstFileName",
         |      "timeStorageClassUpdated":"2018-11-24T10:24:13.992Z",
         |      "storageClass":"MULTI_REGIONAL",
         |      "id":"alpakka/GoogleCloudStorageClientIntegrationSpec63f9feb6-e800-472b-a51f-48e1c6d5d43f/testa.txt/1543055053992768",
         |      "contentType":"text/plain; charset=UTF-8",
         |      "updated":"2018-11-24T10:24:13.992Z",
         |      "mediaLink":"https://www.googleapis.com/download/storage/v1/b/alpakka/o/$firstFileName?generation=1543055053992768&alt=media",
         |      "bucket":"alpakka",
         |      "kind":"storage#object",
         |      "metageneration":"1"
         |    }
         |  ]
         |}""".stripMargin

    val listJsonItemsPageTwo =
      s"""{
         |  "kind":"storage#objects",
         |  "items":[
         |    {
         |      "etag":"CMDm8oLo7N4CEAE=",
         |      "name":"$secondFileName",
         |      "size":"5",
         |      "generation":"1543055053992768",
         |      "crc32c":"AtvFhg==",
         |      "md5Hash":"emjwm9mSZxuzsZpecLeCfg==",
         |      "timeCreated":"2018-11-24T10:24:13.992Z",
         |      "selfLink":"https://www.googleapis.com/storage/v1/b/alpakka/o/$secondFileName",
         |      "timeStorageClassUpdated":"2018-11-24T10:24:13.992Z",
         |      "storageClass":"MULTI_REGIONAL",
         |      "id":"alpakka/GoogleCloudStorageClientIntegrationSpec63f9feb6-e800-472b-a51f-48e1c6d5d43f/testa.txt/1543055053992768",
         |      "contentType":"text/plain; charset=UTF-8",
         |      "updated":"2018-11-24T10:24:13.992Z",
         |      "mediaLink":"https://www.googleapis.com/download/storage/v1/b/alpakka/o/$secondFileName?generation=1543055053992768&alt=media",
         |      "bucket":"alpakka",
         |      "kind":"storage#object",
         |      "metageneration":"1"
         |    }
         |  ]
         |}""".stripMargin

    mock.register(
      WireMock
        .get(
          urlEqualTo(s"/b/$bucketName/o" + folder.map(f => s"?prefix=$f").getOrElse(""))
        )
        .withHeader("Authorization", WireMock.equalTo("Bearer " + TestCredentials.accessToken))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withBody(listJsonItemsPageOne)
            .withHeader("Content-Type", "application/json")
        )
    )

    mock.register(
      WireMock
        .get(
          urlEqualTo(s"/b/$bucketName/o?pageToken=$nextPageToken" + folder.map(f => s"&prefix=$f").getOrElse(""))
        )
        .withHeader("Authorization", WireMock.equalTo("Bearer " + TestCredentials.accessToken))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withBody(listJsonItemsPageTwo)
            .withHeader("Content-Type", "application/json")
        )
    )
  }

  def mockBucketListingFailure(): Unit =
    mock.register(
      WireMock
        .get(
          urlEqualTo(s"/b/$bucketName/o")
        )
        .withHeader("Authorization", WireMock.equalTo("Bearer " + TestCredentials.accessToken))
        .willReturn(
          aResponse()
            .withStatus(400)
            .withBody("Bucket listing failed")
        )
    )

  def mockGetExistingStorageObject(): Unit =
    mock.register(
      WireMock
        .get(
          urlEqualTo(s"/b/$bucketName/o/$fileName")
        )
        .withHeader("Authorization", WireMock.equalTo("Bearer " + TestCredentials.accessToken))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withBody(storageObjectJson)
            .withHeader("Content-Type", "application/json")
        )
    )

  def mockGetNonExistingStorageObject(): Unit =
    mock.register(
      WireMock
        .get(
          urlEqualTo(s"/b/$bucketName/o/$fileName")
        )
        .withHeader("Authorization", WireMock.equalTo("Bearer " + TestCredentials.accessToken))
        .willReturn(
          aResponse()
            .withStatus(404)
        )
    )

  def mockGetNonStorageObjectFailure(): Unit =
    mock.register(
      WireMock
        .get(
          urlEqualTo(s"/b/$bucketName/o/$fileName")
        )
        .withHeader("Authorization", WireMock.equalTo("Bearer " + TestCredentials.accessToken))
        .willReturn(
          aResponse()
            .withStatus(400)
            .withBody("Get storage object failed")
        )
    )

  def mockFileDownload(fileContent: String): Unit =
    mock.register(
      WireMock
        .get(
          urlEqualTo(s"/b/$bucketName/o/$fileName?alt=media")
        )
        .withHeader("Authorization", WireMock.equalTo("Bearer " + TestCredentials.accessToken))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withBody(fileContent.getBytes)
        )
    )

  def mockNonExistingFileDownload(): Unit =
    mock.register(
      WireMock
        .get(
          urlEqualTo(s"/b/$bucketName/o/$fileName?alt=media")
        )
        .withHeader("Authorization", WireMock.equalTo("Bearer " + TestCredentials.accessToken))
        .willReturn(
          aResponse()
            .withStatus(404)
        )
    )

  def mockFileDownloadFailure(): Unit =
    mock.register(
      WireMock
        .get(
          urlEqualTo(s"/b/$bucketName/o/$fileName?alt=media")
        )
        .withHeader("Authorization", WireMock.equalTo("Bearer " + TestCredentials.accessToken))
        .willReturn(
          aResponse()
            .withStatus(400)
            .withBody("File download failed")
        )
    )

  def mockUploadSmallFile(fileContent: String): Unit =
    mock.register(
      WireMock
        .post(
          urlEqualTo(s"/upload/b/$bucketName/o?uploadType=media&name=$fileName")
        )
        .withRequestBody(matching(fileContent))
        .withHeader("Authorization", WireMock.equalTo("Bearer " + TestCredentials.accessToken))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(storageObjectJson)
        )
    )

  def mockUploadSmallFileFailure(fileContent: String): Unit =
    mock.register(
      WireMock
        .post(
          urlEqualTo(s"/upload/b/$bucketName/o?uploadType=media&name=$fileName")
        )
        .withRequestBody(matching(fileContent))
        .withHeader("Authorization", WireMock.equalTo("Bearer " + TestCredentials.accessToken))
        .willReturn(
          aResponse()
            .withStatus(400)
            .withBody("Upload small file failed")
        )
    )

  def mockLargeFileUpload(firstChunkContent: String, secondChunkContent: String, chunkSize: Int): Unit = {
    val uploadId = "uploadId"

    mock.register(
      WireMock
        .post(
          urlEqualTo(s"/upload/b/$bucketName/o?uploadType=resumable&name=$fileName")
        )
        .withHeader("Authorization", WireMock.equalTo("Bearer " + TestCredentials.accessToken))
        .withHeader("Content-Length", WireMock.equalTo("0"))
        .willReturn(
          aResponse()
            .withHeader(
              "Location",
              s"http://localhost:${_wireMockServer.port()}//upload/storage/v1/b/myBucket/o?uploadType=resumable&upload_id=$uploadId"
            )
            .withStatus(200)
        )
    )

    mock.register(
      WireMock
        .put(
          urlEqualTo(s"/upload/b/$bucketName/o?uploadType=resumable&name=$fileName&upload_id=$uploadId")
        )
        .withHeader("Authorization", WireMock.equalTo("Bearer " + TestCredentials.accessToken))
        .withHeader("Content-Length", WireMock.equalTo(s"$chunkSize"))
        .withHeader("Content-Range", WireMock.equalTo(s"bytes 0-${chunkSize - 1}/*"))
        .withRequestBody(WireMock.equalTo(firstChunkContent))
        .willReturn(
          aResponse()
            .withHeader(
              "Location",
              s"http://localhost:${_wireMockServer.port()}//upload/storage/v1/b/myBucket/o?uploadType=resumable&upload_id=$uploadId"
            )
            .withStatus(308)
        )
    )

    mock.register(
      WireMock
        .put(
          urlEqualTo(s"/upload/b/$bucketName/o?uploadType=resumable&name=$fileName&upload_id=$uploadId")
        )
        .withHeader("Authorization", WireMock.equalTo("Bearer " + TestCredentials.accessToken))
        .withHeader("Content-Length", WireMock.equalTo(s"$chunkSize"))
        .withHeader("Content-Range", WireMock.equalTo(s"bytes $chunkSize-${2 * chunkSize - 1}/524288"))
        .withRequestBody(WireMock.equalTo(secondChunkContent))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(storageObjectJson)
        )
    )
  }

  def mockLargeFileUploadFailure(firstChunkContent: String, secondChunkContent: String, chunkSize: Int): Unit = {
    val uploadId = "uploadId"

    mock.register(
      WireMock
        .post(
          urlEqualTo(s"/upload/b/$bucketName/o?uploadType=resumable&name=$fileName")
        )
        .withHeader("Authorization", WireMock.equalTo("Bearer " + TestCredentials.accessToken))
        .withHeader("Content-Length", WireMock.equalTo("0"))
        .willReturn(
          aResponse()
            .withHeader(
              "Location",
              s"http://localhost:${_wireMockServer.port()}//upload/storage/v1/b/myBucket/o?uploadType=resumable&upload_id=$uploadId"
            )
            .withStatus(200)
        )
    )

    mock.register(
      WireMock
        .put(
          urlEqualTo(s"/upload/b/$bucketName/o?uploadType=resumable&name=$fileName&upload_id=$uploadId")
        )
        .withHeader("Authorization", WireMock.equalTo("Bearer " + TestCredentials.accessToken))
        .withHeader("Content-Length", WireMock.equalTo(s"$chunkSize"))
        .withHeader("Content-Range", WireMock.equalTo(s"bytes 0-${chunkSize - 1}/*"))
        .withRequestBody(WireMock.equalTo(firstChunkContent))
        .willReturn(
          aResponse()
            .withHeader(
              "Location",
              s"http://localhost:${_wireMockServer.port()}//upload/storage/v1/b/myBucket/o?uploadType=resumable&upload_id=$uploadId"
            )
            .withStatus(308)
        )
    )

    mock.register(
      WireMock
        .put(
          urlEqualTo(s"/upload/b/$bucketName/o?uploadType=resumable&name=$fileName&upload_id=$uploadId")
        )
        .withHeader("Authorization", WireMock.equalTo("Bearer " + TestCredentials.accessToken))
        .withHeader("Content-Length", WireMock.equalTo(s"$chunkSize"))
        .withHeader("Content-Range", WireMock.equalTo(s"bytes $chunkSize-${2 * chunkSize - 1}/524288"))
        .withRequestBody(WireMock.equalTo(secondChunkContent))
        .willReturn(
          aResponse()
            .withStatus(400)
            .withBody("Chunk upload failed")
        )
    )
  }

  def mockRewrite(rewriteBucketName: String): Unit = {
    val rewriteToken = "rewriteToken"

    val rewriteStorageObjectJson =
      s"""
         |{
         |  "etag":"CMDm8oLo7N4CEAE=",
         |  "name":"$fileName",
         |  "size":"5",
         |  "generation":"1543055053992768",
         |  "crc32c":"AtvFhg==",
         |  "md5Hash":"emjwm9mSZxuzsZpecLeCfg==",
         |  "timeCreated":"2018-11-24T10:24:13.992Z",
         |  "selfLink":"https://www.googleapis.com/storage/v1/b/alpakka/o/$fileName",
         |  "timeStorageClassUpdated":"2018-11-24T10:24:13.992Z",
         |  "storageClass":"MULTI_REGIONAL",
         |  "id":"alpakka/GoogleCloudStorageClientIntegrationSpec63f9feb6-e800-472b-a51f-48e1c6d5d43f/testa.txt/1543055053992768",
         |  "contentType":"text/plain; charset=UTF-8",
         |  "updated":"2018-11-24T10:24:13.992Z",
         |  "mediaLink":"https://www.googleapis.com/download/storage/v1/b/alpakka/o/$fileName?generation=1543055053992768&alt=media",
         |  "bucket":"$rewriteBucketName",
         |  "kind":"storage#object",
         |  "metageneration":"1"
         |}""".stripMargin

    val firstRewriteResponse =
      s"""
         |{
         |  "kind": "storage#rewriteResponse",
         |  "totalBytesRewritten": "100000",
         |  "objectSize": "200000",
         |  "done": false,
         |  "rewriteToken": "$rewriteToken"
         |}
       """.stripMargin

    val secondRewriteResponse =
      s"""
         |{
         |  "kind": "storage#rewriteResponse",
         |  "totalBytesRewritten": "200000",
         |  "objectSize": "200000",
         |  "done": true,
         |  "resource": $rewriteStorageObjectJson
         |}
       """.stripMargin

    mock.register(
      WireMock
        .post(
          urlEqualTo(s"/b/$bucketName/o/$fileName/rewriteTo/b/$rewriteBucketName/o/$fileName")
        )
        .withHeader("Authorization", WireMock.equalTo("Bearer " + TestCredentials.accessToken))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withBody(firstRewriteResponse)
            .withHeader("Content-Type", "application/json")
        )
    )

    mock.register(
      WireMock
        .post(
          urlEqualTo(
            s"/b/$bucketName/o/$fileName/rewriteTo/b/$rewriteBucketName/o/$fileName?rewriteToken=$rewriteToken"
          )
        )
        .withHeader("Authorization", WireMock.equalTo("Bearer " + TestCredentials.accessToken))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withBody(secondRewriteResponse)
            .withHeader("Content-Type", "application/json")
        )
    )
  }

  def mockRewriteFailure(rewriteBucketName: String): Unit = {
    val rewriteToken = "rewriteToken"
    val firstRewriteResponse =
      s"""
         |{
         |  "kind": "storage#rewriteResponse",
         |  "totalBytesRewritten": "100000",
         |  "objectSize": "200000",
         |  "done": false,
         |  "rewriteToken": "$rewriteToken"
         |}
       """.stripMargin

    mock.register(
      WireMock
        .post(
          urlEqualTo(s"/b/$bucketName/o/$fileName/rewriteTo/b/$rewriteBucketName/o/$fileName")
        )
        .withHeader("Authorization", WireMock.equalTo("Bearer " + TestCredentials.accessToken))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withBody(firstRewriteResponse)
            .withHeader("Content-Type", "application/json")
        )
    )

    mock.register(
      WireMock
        .post(
          urlEqualTo(
            s"/b/$bucketName/o/$fileName/rewriteTo/b/$rewriteBucketName/o/$fileName?rewriteToken=$rewriteToken"
          )
        )
        .withHeader("Authorization", WireMock.equalTo("Bearer " + TestCredentials.accessToken))
        .willReturn(
          aResponse()
            .withStatus(400)
            .withBody("Rewrite failed")
        )
    )

  }
}

object GCStorageWiremockBase {

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
    )
    server.start()
    server
  }

  private def config(proxyPort: Int) =
    ConfigFactory.parseString(s"""
    |${GCStorageSettings.ConfigPath} {
    |  project-id = ""testX-XXXXX""
    |  client-email = "test-XXX@test-XXXXX.iam.gserviceaccount.com"
    |  private-key = \"\"\"
    |-----BEGIN PRIVATE KEY-----
    |MIICeAIBADANBgkqhkiG9w0BAQEFAASCAmIwggJeAgEAAoGBAMwkmdwrWp+LLlsf
    |bVE+neFjZtUNuaD4/tpQ2UIh2u+qU6sr4bG8PPuqSdrt5b0/0vfMZA11mQWmKpg5
    |PK98kEkhbSvC08fG0TtpR9+vflghOuuvcw6kCniwNbHlOXnE8DwtKQp1DbTUPzMD
    |hhsIjJaUtv19Xk7gh4MqYgANTm6lAgMBAAECgYEAwBXIeHSKxwiNS8ycbg//Oq7v
    |eZV6j077bq0YYLO+cDjSlYOq0DSRJTSsXcXvoE1H00aM9mUq4TfjaGyi/3SzxYsr
    |rSzu/qpYC58MJsnprIjlLgFZmZGe5MOSoul/u6JsBTJGkYPV0xGrtXJY103aSYzC
    |xthpY0BHy9eO9I/pNlkCQQD/64g4INAiBdM4R5iONQvh8LLvqbb8Bw4vVwVFFnAr
    |YHcomxtT9TunMad6KPgbOCd/fTttDADrv54htBrFGXeXAkEAzDTtisPKXPByJnUd
    |jKO2oOg0Fs9IjGeWbnkrsN9j0134ldARE+WbT5S8G5EFo+bQi4ffU3+Y/4ly6Amm
    |OAAzIwJBANV2GAD5HaHDShK/ZTf4dxjWM+pDnSVKnUJPS039EUKdC8cK2RiGjGNA
    |v3jdg1Tw2cE1K8QhJwN8qOFj4JBWVbECQQCwcntej9bnf4vi1wd1YnCHkJyRqQIS
    |7974DhNGfYAQPv5w1JwtCRSuKuJvH1w0R1ijd//scjCNfQKgpNXPRbzpAkAQ8MFA
    |MLpOLGqezUQthJWmVtnXEXaAlb3yFSRTZQVEselObiIc6EvYzNXv780IDT4pyKjg
    |8DS9i5jJDIVWr7mA
    |-----END PRIVATE KEY-----
    |\"\"\"
    |  base-url = "http://localhost:${proxyPort}"
    |  base-path = ""
    |  token-url = "http://localhost:${proxyPort}/oauth2/v4/token"
    |}
    """.stripMargin)
}

object TestCredentials {
  val accessToken =
    "ya29.Elz4A2XkfGKJ4CoS5x_umUBHsvjGdeWQzu6gRRCnNXI0fuIyoDP_6aYktBQEOI4YAhLNgUl2OpxWQaN8Z3hd5YfFw1y4EGAtr2o28vSID-c8ul_xxHuudE7RmhH9sg"
}
