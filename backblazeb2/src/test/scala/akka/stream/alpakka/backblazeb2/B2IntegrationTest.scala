/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.backblazeb2

import java.nio.charset.StandardCharsets
import java.util.UUID

import akka.stream.alpakka.backblazeb2.Protocol._
import akka.util.ByteString
import org.scalatest.Assertion
import org.scalatest.Matchers._

trait B2IntegrationTest {
  private def readEnv(key: String): String =
    Option(System.getenv(key)) getOrElse sys.error(s"Please set $key environment variable to run the tests")

  def checkDownloadResponse(result: DownloadFileResponse): Assertion = {
    val receivedText = new String(result.data.toArray, StandardCharsets.UTF_8.name)
    receivedText shouldEqual dataText
  }

  val accountId = AccountId(readEnv("B2_ACCOUNT_ID"))
  val applicationKey = ApplicationKey(readEnv("B2_APPLICATION_KEY"))
  val bucketName = BucketName("alpakka-test") // TODO: create new using b2_create_bucket then remove after
  val bucketId = BucketId(readEnv("B2_BUCKET_ID")) // TODO: read using b2_list_buckets API call

  val credentials = B2AccountCredentials(accountId, applicationKey)

  val testRun = UUID.randomUUID().toString
  val fileName = FileName(s"test-$testRun.txt")

  val dataText = s"This is test data '$testRun'!"
  val dataByteString = ByteString(dataText.getBytes(StandardCharsets.UTF_8.name))
}
