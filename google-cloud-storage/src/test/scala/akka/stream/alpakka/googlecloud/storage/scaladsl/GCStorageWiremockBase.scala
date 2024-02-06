/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.storage.scaladsl

import akka.actor.ActorSystem
import akka.stream.alpakka.google.GoogleSettings
import akka.stream.alpakka.googlecloud.storage.GCStorageSettings
import akka.stream.alpakka.googlecloud.storage.scaladsl.GCStorageWiremockBase._
import akka.testkit.TestKit
import com.typesafe.config.ConfigFactory
import io.specto.hoverfly.junit.core.SimulationSource.dsl
import io.specto.hoverfly.junit.core.{Hoverfly, HoverflyConfig, HoverflyMode, SimulationSource}
import io.specto.hoverfly.junit.dsl.HoverflyDsl.{response, service}
import io.specto.hoverfly.junit.dsl.matchers.HoverflyMatchers.equalsToJson
import spray.json.DefaultJsonProtocol.{mapFormat, StringJsonFormat}
import spray.json.enrichAny

import scala.annotation.nowarn
import scala.util.Random

abstract class GCStorageWiremockBase(_system: ActorSystem, _wireMockServer: Hoverfly) extends TestKit(_system) {

  def this(mock: Hoverfly) =
    this(ActorSystem(getCallerName(classOf[GCStorageWiremockBase]),
                     config(mock.getHoverflyConfig.getProxyPort).withFallback(ConfigFactory.load())
         ),
         mock
    )

  def this() = this(initServer())

  val port = _wireMockServer.getHoverflyConfig.getProxyPort
  val mock = _wireMockServer

  def stopWireMockServer(): Unit = _wireMockServer.close()

  val bucketName = "alpakka"
  val fileName = "file1.txt"
  val generation = 1543055053992769L

  def storageObjectJson(
      generation: Long = 1543055053992768L,
      metadata: Map[String, String] = Map("countryOfOrigin" -> "United Kingdom"),
      maybeMd5Hash: Option[String] = Some("emjwm9mSZxuzsZpecLeCfg=="),
      maybeCrc32c: Option[String] = Some("AtvFhg==")
  ): String =
    s"""
       |{
       |  "etag":"CMDm8oLo7N4CEAE=",
       |  "name":"$fileName",
       |  "size":"5",
       |  "generation":"$generation",
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
       |  "metageneration":"1",
       |  "timeDeleted": "2018-11-24T10:24:13.992Z",
       |  "temporaryHold": false,
       |  "eventBasedHold": false,
       |  "retentionExpirationTime": "2018-11-24T10:24:13.992Z",
       |  "contentEncoding": "identity",
       |  "contentDisposition": "inline",
       |  "contentLanguage": "en-GB",
       |  "cacheControl": "no-cache",
       |  "componentCount": 2,
       |  "customTime": "2020-09-17T11:09:21.039Z",
       |  "kmsKeyName": "projects/my-gcs-project/keys",
       |  "metadata": ${metadata.toJson.compactPrint},
       |  "customerEncryption": {"encryptionAlgorithm": "AES256", "keySha256": "encryption-key-sha256"},
       |  "owner": {"entity": "project-owners-123412341234", "entityId": "790607247"},
       |  "acl": [{ "kind": "storage#objectAccessControl", "id": "my-bucket/test-acl/1463505795940000", "selfLink": "https://www.googleapis.com/storage/v1/b/my-bucket/o/test-acl", "bucket": "my-bucket", "object": "test-acl", "generation": "1463505795940000", "entity": "some-entity", "role": "OWNER", "email": "owner@google.com", "entityId": "790607247", "domain": "my-domain", "projectTeam": { "projectNumber": "57959400", "team": "management" }, "etag": "R2OZrfQiij=" }]
       |  ${maybeMd5Hash.fold("")(hash => s""","md5Hash":"$hash"""")}
       |  ${maybeCrc32c.fold("")(crc32c => s""","crc32c":"$crc32c"""")}
       | }
       |""".stripMargin

  def getRandomString(size: Int): String =
    Random.alphanumeric.take(size).mkString

  private implicit final class InplaceOp[T](t: T) {
    def inplace(f: T => Unit): T = {
      f(t)
      t
    }
  }

  def mockTokenApi: SimulationSource = {
    dsl(
      service("oauth2.googleapis.com")
        .post("/token")
        .anyQueryParams()
        .anyBody()
        .willReturn(
          response()
            .header("Content-Type", "application/json")
            .body(s"""{"access_token": "${TestCredentials.accessToken}", "token_type": "String", "expires_in": 3600}""")
        )
    )
  }

  def storageService = service("storage.googleapis.com")

  def mockBucketCreate(location: String) = {
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

    dsl(
      storageService
        .post("/storage/v1/b")
        .queryParam("prettyPrint", "false")
        .queryParam("project", "testX-XXXXX")
        .body(equalsToJson(createBucketJsonRequest))
        .willReturn(
          response()
            .header("Content-Type", "application/json")
            .body(createBucketJsonResponse)
        )
    )
  }

  def mockBucketCreateFailure(location: String) = {
    val createBucketJsonRequest = s"""{"name":"$bucketName","location":"$location"}"""
    dsl(
      storageService
        .post("/storage/v1/b")
        .queryParam("prettyPrint", "false")
        .queryParam("project", "testX-XXXXX")
        .body(equalsToJson(createBucketJsonRequest))
        .willReturn(
          response()
            .status(400)
            .body("Create failed")
        )
    )
  }

  def mockDeleteBucket() = {
    val deleteBucketJsonResponse = """{"kind":"storage#objects"}"""

    dsl(
      storageService
        .delete(s"/storage/v1/b/$bucketName")
        .queryParam("prettyPrint", "false")
        .willReturn(
          response()
            .status(204)
            .body(deleteBucketJsonResponse)
            .header("Content-Type", "application/json")
        )
    )
  }

  def mockDeleteBucketFailure() =
    dsl(
      storageService
        .delete(s"/storage/v1/b/$bucketName")
        .queryParam("prettyPrint", "false")
        .willReturn(
          response()
            .status(400)
            .body("Delete failed")
            .header("Content-Type", "application/json")
        )
    )

  def mockGetExistingBucket() = {
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

    dsl(
      storageService
        .get(s"/storage/v1/b/$bucketName")
        .queryParam("prettyPrint", "false")
        .willReturn(
          response()
            .status(200)
            .body(getBucketJsonResponse)
            .header("Content-Type", "application/json")
        )
    )
  }

  def mockGetNonExistingBucket() =
    dsl(
      storageService
        .get(s"/storage/v1/b/$bucketName")
        .queryParam("prettyPrint", "false")
        .willReturn(
          response()
            .status(404)
            .header("Content-Type", "application/json")
        )
    )

  def mockGetBucketFailure() =
    dsl(
      storageService
        .get(s"/storage/v1/b/$bucketName")
        .queryParam("prettyPrint", "false")
        .willReturn(
          response()
            .status(400)
            .body("Get bucket failed")
            .header("Content-Type", "application/json")
        )
    )

  def mockEmptyBucketListing() = {
    val emptyBucketJsonResponse = """{"kind":"storage#objects"}"""

    dsl(
      storageService
        .get(s"/storage/v1/b/$bucketName/o")
        .queryParam("prettyPrint", "false")
        .willReturn(
          response()
            .status(200)
            .body(emptyBucketJsonResponse)
            .header("Content-Type", "application/json")
        )
    )
  }

  def mockNonExistingFolderListing(folder: String) = {
    val emptyFolderJsonResponse = """{"kind":"storage#objects"}"""

    dsl(
      storageService
        .get(s"/storage/v1/b/$bucketName/o")
        .queryParam("prettyPrint", "false")
        .queryParam("prefix", folder)
        .willReturn(
          response()
            .status(200)
            .body(emptyFolderJsonResponse)
            .header("Content-Type", "application/json")
        )
    )
  }

  def mockDeleteObjectJava(name: String) =
    mockDeleteObject(name, None)

  def mockDeleteObjectJava(name: String, generation: Long) =
    mockDeleteObject(name, Some(generation))

  def mockDeleteObject(name: String, generation: Option[Long] = None) =
    dsl(
      storageService
        .delete(s"/storage/v1/b/$bucketName/o/$name")
        .queryParam("prettyPrint", "false")
        .inplace(b => generation.foreach(g => b.queryParam("generation", g.toString)))
        .willReturn(
          response()
            .status(204)
        )
    )

  def mockNonExistingDeleteObject(name: String) =
    dsl(
      storageService
        .delete(s"/storage/v1/b/$bucketName/o/$name")
        .queryParam("prettyPrint", "false")
        .willReturn(
          response()
            .status(404)
        )
    )

  def mockDeleteObjectFailure(name: String) =
    dsl(
      storageService
        .delete(s"/storage/v1/b/$bucketName/o/$name")
        .queryParam("prettyPrint", "false")
        .willReturn(
          response()
            .status(400)
            .body("Delete object failed")
        )
    )

  def mockObjectExists(name: String) =
    dsl(
      storageService
        .head(s"/storage/v1/b/$bucketName/o/$name")
        .queryParam("prettyPrint", "false")
        .willReturn(
          response()
            .status(200)
        )
    )

  def mockObjectDoesNotExist(name: String) =
    dsl(
      storageService
        .head(s"/storage/v1/b/$bucketName/o/$name")
        .queryParam("prettyPrint", "false")
        .willReturn(
          response()
            .status(404)
        )
    )

  def mockObjectExistsFailure(name: String) =
    dsl(
      storageService
        .head(s"/storage/v1/b/$bucketName/o/$name")
        .queryParam("prettyPrint", "false")
        .willReturn(
          response()
            .status(400)
        )
    )

  def mockNonExistingBucketListing(folder: Option[String] = None) =
    dsl(
      storageService
        .get(s"/storage/v1/b/$bucketName/o")
        .queryParam("prettyPrint", "false")
        .inplace(b => folder.foreach(f => b.queryParam("prefix", f)))
        .willReturn(
          response()
            .status(404)
        )
    )

  def mockNonExistingBucketListingJava(folder: String) =
    mockNonExistingBucketListing(Some(folder))

  def mockNonExistingBucketListingJava() =
    mockNonExistingBucketListing(None)

  def mockBucketListingJava(firstFileName: String, secondFileName: String) =
    mockBucketListing(firstFileName, secondFileName)

  def mockBucketListingJava(firstFileName: String, secondFileName: String, folder: String) =
    mockBucketListing(firstFileName, secondFileName, Some(folder))

  def mockBucketListingJava(firstFileName: String, secondFileName: String, folder: String, versions: Boolean) =
    mockBucketListing(firstFileName, secondFileName, Some(folder), versions)

  def mockBucketListing(firstFileName: String,
                        secondFileName: String,
                        folder: Option[String] = None,
                        versions: Boolean = false
  ) = {
    val nextPageToken = "CiAyMDA1MDEwMy8wMDAwOTUwMTQyLTA1LTAwMDAwNi5uYw"

    val firstFile =
      s"""{
         |  "etag":"CMDm8oLo7N4CEAE=",
         |  "name":"$firstFileName",
         |  "size":"5",
         |  "generation":"1543055053992768",
         |  "crc32c":"AtvFhg==",
         |  "md5Hash":"emjwm9mSZxuzsZpecLeCfg==",
         |  "timeCreated":"2018-11-24T10:24:13.992Z",
         |  "selfLink":"https://www.googleapis.com/storage/v1/b/alpakka/o/$firstFileName",
         |  "timeStorageClassUpdated":"2018-11-24T10:24:13.992Z",
         |  "storageClass":"MULTI_REGIONAL",
         |  "id":"alpakka/GoogleCloudStorageClientIntegrationSpec63f9feb6-e800-472b-a51f-48e1c6d5d43f/testa.txt/1543055053992768",
         |  "contentType":"text/plain; charset=UTF-8",
         |  "updated":"2018-11-24T10:24:13.992Z",
         |  "mediaLink":"https://www.googleapis.com/download/storage/v1/b/alpakka/o/$firstFileName?generation=1543055053992768&alt=media",
         |  "bucket":"alpakka",
         |  "kind":"storage#object",
         |   "metageneration":"1",
         |  "timeDeleted": "2018-11-24T10:24:13.992Z",
         |  "temporaryHold": false,
         |  "eventBasedHold": false,
         |  "retentionExpirationTime": "2018-11-24T10:24:13.992Z",
         |  "contentEncoding": "identity",
         |  "contentDisposition": "inline",
         |  "contentLanguage": "en-GB",
         |  "cacheControl": "no-cache"
         |}""".stripMargin

    val firstFileArchived =
      s"""{
         |  "etag":"CMDm8oLo7N4CEAE=",
         |  "name":"$firstFileName#$generation",
         |  "size":"5",
         |  "generation":"$generation",
         |  "crc32c":"AtvFhg==",
         |  "md5Hash":"emjwm9mSZxuzsZpecLeCfg==",
         |  "timeCreated":"2018-11-24T10:24:13.992Z",
         |  "selfLink":"https://www.googleapis.com/storage/v1/b/alpakka/o/$firstFileName",
         |  "timeStorageClassUpdated":"2018-11-24T10:24:13.992Z",
         |  "storageClass":"MULTI_REGIONAL",
         |  "id":"alpakka/GoogleCloudStorageClientIntegrationSpec63f9feb6-e800-472b-a51f-48e1c6d5d43f/testa.txt/1543055053992768",
         |  "contentType":"text/plain; charset=UTF-8",
         |  "updated":"2018-11-24T10:24:13.992Z",
         |  "mediaLink":"https://www.googleapis.com/download/storage/v1/b/alpakka/o/$firstFileName?generation=1543055053992768&alt=media",
         |  "bucket":"alpakka",
         |  "kind":"storage#object",
         |  "metageneration":"1",
         |  "timeDeleted": "2018-11-24T10:24:13.992Z",
         |  "temporaryHold": false,
         |  "eventBasedHold": false,
         |  "retentionExpirationTime": "2018-11-24T10:24:13.992Z",
         |  "contentEncoding": "identity",
         |  "contentDisposition": "inline",
         |  "contentLanguage": "en-GB",
         |  "cacheControl": "no-cache"
         |}""".stripMargin

    val listJsonItemsPageOne =
      s"""{
         |  "kind":"storage#objects",
         |  "nextPageToken": "$nextPageToken",
         |  "items":[
         |    $firstFile ${if (versions) s", $firstFileArchived" else ""}
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
         |      "metageneration":"1",
         |      "timeDeleted": "2018-11-24T10:24:13.992Z",
         |      "temporaryHold": false,
         |      "eventBasedHold": false,
         |      "retentionExpirationTime": "2018-11-24T10:24:13.992Z",
         |      "contentEncoding": "identity",
         |      "contentDisposition": "inline",
         |      "contentLanguage": "en-GB",
         |      "cacheControl": "no-cache"
         |    }
         |  ]
         |}""".stripMargin

    dsl(
      storageService
        .get(s"/storage/v1/b/$bucketName/o")
        .queryParam("prettyPrint", "false")
        .inplace(b => folder.foreach(f => b.queryParam("prefix", f)))
        .inplace(b => { if (versions) b.queryParam("versions", "true") })
        .willReturn(
          response()
            .status(200)
            .body(listJsonItemsPageOne)
            .header("Content-Type", "application/json")
        )
        .get(s"/storage/v1/b/$bucketName/o")
        .queryParam("prettyPrint", "false")
        .inplace(b => folder.foreach(f => b.queryParam("prefix", f)))
        .inplace(b => { if (versions) b.queryParam("versions", "true") })
        .queryParam("pageToken", nextPageToken)
        .willReturn(
          response()
            .status(200)
            .body(listJsonItemsPageTwo)
            .header("Content-Type", "application/json")
        )
    )
  }

  def mockBucketListingFailure() =
    dsl(
      storageService
        .get(s"/storage/v1/b/$bucketName/o")
        .queryParam("prettyPrint", "false")
        .willReturn(
          response()
            .status(400)
            .body("Bucket listing failed")
        )
    )

  def mockGetExistingStorageObjectJava() =
    mockGetExistingStorageObject()

  def mockGetExistingStorageObjectJava(generation: Long) =
    mockGetExistingStorageObject(Some(generation))

  def mockGetExistingStorageObject(generation: Option[Long] = None,
                                   maybeMd5Hash: Option[String] = Some("emjwm9mSZxuzsZpecLeCfg=="),
                                   maybeCrc32c: Option[String] = Some("AtvFhg==")
  ) =
    dsl(
      storageService
        .get(s"/storage/v1/b/$bucketName/o/$fileName")
        .queryParam("prettyPrint", "false")
        .inplace(b => generation.foreach(g => b.queryParam("generation", g.toString)))
        .willReturn(
          response()
            .status(200)
            .body(
              generation
                .map(
                  storageObjectJson(_, maybeMd5Hash = maybeMd5Hash, maybeCrc32c = maybeCrc32c)
                ) getOrElse storageObjectJson(
                maybeMd5Hash = maybeMd5Hash,
                maybeCrc32c = maybeCrc32c
              )
            )
            .header("Content-Type", "application/json")
        )
    )

  def mockGetNonExistingStorageObject() =
    dsl(
      storageService
        .get(s"/storage/v1/b/$bucketName/o/$fileName")
        .queryParam("prettyPrint", "false")
        .willReturn(
          response()
            .status(404)
        )
    )

  def mockGetNonStorageObjectFailure() =
    dsl(
      storageService
        .get(s"/storage/v1/b/$bucketName/o/$fileName")
        .queryParam("prettyPrint", "false")
        .willReturn(
          response()
            .status(400)
            .body("Get storage object failed")
        )
    )

  def mockFileDownloadJava(fileContent: String) =
    mockFileDownload(fileContent)

  def mockFileDownloadJava(fileContent: String, generation: Long) =
    mockFileDownload(fileContent, Some(generation))

  def mockFileDownload(fileContent: String, generation: Option[Long] = None) =
    dsl(
      storageService
        .get(s"/storage/v1/b/$bucketName/o/$fileName")
        .queryParam("alt", "media")
        .queryParam("prettyPrint", "false")
        .inplace(b => generation.foreach(g => b.queryParam("generation", g.toString)))
        .willReturn(
          response()
            .status(200)
            .body(fileContent)
        )
    )

  def mockNonExistingFileDownload() =
    dsl(
      storageService
        .get(s"/storage/v1/b/$bucketName/o/$fileName")
        .queryParam("alt", "media")
        .queryParam("prettyPrint", "false")
        .willReturn(
          response()
            .status(404)
        )
    )

  def mockFileDownloadFailure() =
    dsl(
      storageService
        .get(s"/storage/v1/b/$bucketName/o/$fileName")
        .queryParam("alt", "media")
        .queryParam("prettyPrint", "false")
        .willReturn(
          response()
            .status(400)
            .body("File download failed")
        )
    )

  def mockFileDownloadFailureThenSuccess(failureStatus: Int, failureMessage: String, fileContent: String) =
    dsl(
      storageService
        .get(s"/storage/v1/b/$bucketName/o/$fileName")
        .queryParam("alt", "media")
        .queryParam("prettyPrint", "false")
        .willReturn(
          response()
            .status(failureStatus)
            .body(failureMessage)
            .andSetState("Retry scenario", "after error")
        )
        .get(s"/storage/v1/b/$bucketName/o/$fileName")
        .queryParam("alt", "media")
        .queryParam("prettyPrint", "false")
        .withState("Retry scenario", "after error")
        .willReturn(
          response()
            .status(200)
            .body(fileContent)
        )
    )

  def mockUploadSmallFile(fileContent: String) =
    dsl(
      storageService
        .post(s"/upload/storage/v1/b/$bucketName/o")
        .queryParam("uploadType", "media")
        .queryParam("name", fileName)
        .queryParam("prettyPrint", "false")
        .body(fileContent)
        .willReturn(
          response()
            .status(200)
            .header("Content-Type", "application/json")
            .body(storageObjectJson())
        )
    )

  def mockUploadSmallFileFailure(fileContent: String) =
    dsl(
      storageService
        .post(s"/upload/storage/v1/b/$bucketName/o")
        .queryParam("uploadType", "media")
        .queryParam("name", fileName)
        .queryParam("prettyPrint", "false")
        .body(fileContent)
        .willReturn(
          response()
            .status(400)
            .body("Upload small file failed")
        )
    )

  def mockLargeFileUpload(firstChunkContent: String, secondChunkContent: String, chunkSize: Int): SimulationSource =
    mockLargeFileUpload(firstChunkContent, secondChunkContent, chunkSize, None)

  def mockLargeFileUpload(firstChunkContent: String,
                          secondChunkContent: String,
                          chunkSize: Int,
                          metadata: Option[Map[String, String]] = None
  ) = {
    val uploadId = "uploadId"

    val noMeta = storageService
      .post(s"/upload/storage/v1/b/$bucketName/o")
      .queryParam("uploadType", "resumable")
      .queryParam("name", fileName)
      .queryParam("prettyPrint", "false")

    dsl(
      metadata
        .fold(noMeta) { m =>
          noMeta.body(equalsToJson(m.toJson.toString))
        }
        .willReturn(
          response()
            .header(
              "Location",
              s"https://storage.googleapis.com/upload/storage/v1/b/$bucketName/o?uploadType=resumable&upload_id=$uploadId"
            )
            .status(200)
        )
        .put(s"/upload/storage/v1/b/$bucketName/o")
        .queryParam("uploadType", "resumable")
        .queryParam("upload_id", uploadId)
        .queryParam("prettyPrint", "false")
        .header("Content-Length", s"$chunkSize")
        .header("Content-Range", s"bytes 0-${chunkSize - 1}/*")
        .body(firstChunkContent)
        .willReturn(
          response()
            .header(
              "Location",
              s"https://storage.googleapis.com/upload/storage/v1/b/$bucketName/o?uploadType=resumable&upload_id=$uploadId"
            )
            .status(308)
            .andSetState("resumableUploadStatus", "uploadedFirstChunk")
        )
        .put(s"/upload/storage/v1/b/$bucketName/o")
        .queryParam("uploadType", "resumable")
        .queryParam("upload_id", uploadId)
        .queryParam("prettyPrint", "false")
        .header("Content-Length", s"$chunkSize")
        .header("Content-Range", s"bytes $chunkSize-${2 * chunkSize - 1}/524288")
        .body(secondChunkContent)
        .withState("resumableUploadStatus", "uploadedFirstChunk")
        .willReturn(
          response()
            .status(200)
            .header("Content-Type", "application/json")
            .body(metadata.map(m => storageObjectJson(metadata = m)).getOrElse(storageObjectJson()))
        )
    )
  }

  def mockLargeFileUploadFailure(firstChunkContent: String, secondChunkContent: String, chunkSize: Int) = {
    val uploadId = "uploadId"

    dsl(
      storageService
        .post(s"/upload/storage/v1/b/$bucketName/o")
        .queryParam("uploadType", "resumable")
        .queryParam("name", fileName)
        .queryParam("prettyPrint", "false")
        .willReturn(
          response()
            .header(
              "Location",
              s"https://storage.googleapis.com/upload/storage/v1/b/$bucketName/o?uploadType=resumable&upload_id=$uploadId"
            )
            .status(200)
        )
        .put(s"/upload/storage/v1/b/$bucketName/o")
        .queryParam("uploadType", "resumable")
        .queryParam("upload_id", uploadId)
        .queryParam("prettyPrint", "false")
        .header("Content-Length", s"$chunkSize")
        .header("Content-Range", s"bytes 0-${chunkSize - 1}/*")
        .body(firstChunkContent)
        .willReturn(
          response()
            .header(
              "Location",
              s"https://storage.googleapis.com/upload/storage/v1/b/$bucketName/o?uploadType=resumable&upload_id=$uploadId"
            )
            .status(308)
            .andSetState("resumableUploadStatus", "uploadedFirstChunk")
        )
        .put(s"/upload/storage/v1/b/$bucketName/o")
        .queryParam("uploadType", "resumable")
        .queryParam("upload_id", uploadId)
        .queryParam("prettyPrint", "false")
        .header("Content-Length", s"$chunkSize")
        .header("Content-Range", s"bytes $chunkSize-${2 * chunkSize - 1}/524288")
        .body(secondChunkContent)
        .withState("resumableUploadStatus", "uploadedFirstChunk")
        .willReturn(
          response()
            .status(400)
            .body("Chunk upload failed")
        )
    )
  }

  def mockRewrite(rewriteBucketName: String) = {
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
         |  "metageneration":"1",
         |  "timeDeleted": "2018-11-24T10:24:13.992Z",
         |  "temporaryHold": false,
         |  "eventBasedHold": false,
         |  "retentionExpirationTime": "2018-11-24T10:24:13.992Z",
         |  "contentEncoding": "identity",
         |  "contentDisposition": "inline",
         |  "contentLanguage": "en-GB",
         |  "cacheControl": "no-cache"
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

    dsl(
      storageService
        .post(s"/storage/v1/b/$bucketName/o/$fileName/rewriteTo/b/$rewriteBucketName/o/$fileName")
        .queryParam("prettyPrint", "false")
        .willReturn(
          response()
            .status(200)
            .body(firstRewriteResponse)
            .header("Content-Type", "application/json")
        )
        .post(s"/storage/v1/b/$bucketName/o/$fileName/rewriteTo/b/$rewriteBucketName/o/$fileName")
        .queryParam("rewriteToken", rewriteToken)
        .queryParam("prettyPrint", "false")
        .willReturn(
          response()
            .status(200)
            .body(secondRewriteResponse)
            .header("Content-Type", "application/json")
        )
    )
  }

  def mockRewriteFailure(rewriteBucketName: String) = {
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

    dsl(
      storageService
        .post(s"/storage/v1/b/$bucketName/o/$fileName/rewriteTo/b/$rewriteBucketName/o/$fileName")
        .queryParam("prettyPrint", "false")
        .willReturn(
          response()
            .status(200)
            .body(firstRewriteResponse)
            .header("Content-Type", "application/json")
        )
        .post(s"/storage/v1/b/$bucketName/o/$fileName/rewriteTo/b/$rewriteBucketName/o/$fileName")
        .queryParam("rewriteToken", rewriteToken)
        .queryParam("prettyPrint", "false")
        .willReturn(
          response()
            .status(400)
            .body("Rewrite failed")
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

  def initServer(): Hoverfly = {
    val server = new Hoverfly(
      HoverflyConfig
        .localConfigs()
        .proxyPort(8500)
        .adminPort(8888)
        .captureHeaders("Content-Range", "X-Upload-Content-Type")
        .enableStatefulCapture(),
      HoverflyMode.SIMULATE
    )
    server.start()
    server
  }

  private def config(proxyPort: Int) =
    ConfigFactory.parseString(s"""
    |${(GCStorageSettings: @nowarn("msg=deprecated")).ConfigPath} {
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
    |}
    |${GoogleSettings.ConfigPath} {
    |  retry-settings {
    |    max-retries = 1
    |    max-backoff = 1s
    |  }
    |  forward-proxy {
    |    host = localhost
    |    port = 8500
    |    trust-pem = "src/test/resources/cert.pem"
    |  }
    |}
    """.stripMargin)
}

object TestCredentials {
  val accessToken =
    "ya29.Elz4A2XkfGKJ4CoS5x_umUBHsvjGdeWQzu6gRRCnNXI0fuIyoDP_6aYktBQEOI4YAhLNgUl2OpxWQaN8Z3hd5YfFw1y4EGAtr2o28vSID-c8ul_xxHuudE7RmhH9sg"
}
