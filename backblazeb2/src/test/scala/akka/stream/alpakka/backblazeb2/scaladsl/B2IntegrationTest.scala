/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.backblazeb2.scaladsl

import java.nio.charset.StandardCharsets
import java.util.UUID

import akka.stream.alpakka.backblazeb2.Protocol._
import akka.util.ByteString
import org.scalatest.Assertion
import org.scalatest.Matchers._

/**
 * The integration tests which use this trait require a valid Backblaze B2 account with a test bucket
 * created.
 *
 * These integration assume that 4 environment variables will be set, obtained from Backblaze B2
 * admin console at https://secure.backblaze.com/:
 * B2_ACCOUNT_ID - obtained using "Show Account ID and Application Key" link
 * B2_APPLICATION_KEY - obtained using "Show Account ID and Application Key" link
 * B2_BUCKET_NAME - name provided when creating a test bucket under "Bucket Unique Name"
 * B2_BUCKET_ID - "Bucket ID" field seen when viewing the test bucket
 */
trait B2IntegrationTest {
  private def readEnv(key: String): String =
    Option(System.getenv(key)) getOrElse sys.error(s"Please set $key environment variable to run the tests")

  def checkDownloadResponse(result: DownloadFileResponse): Assertion = {
    val receivedText = new String(result.data.toArray, StandardCharsets.UTF_8.name)
    result.fileName shouldEqual fileName
    receivedText shouldEqual dataText
  }

  val accountId = AccountId(readEnv("B2_ACCOUNT_ID"))
  val applicationKey = ApplicationKey(readEnv("B2_APPLICATION_KEY"))
  val bucketName = BucketName(readEnv("B2_BUCKET_NAME"))
  val bucketId = BucketId(readEnv("B2_BUCKET_ID"))

  val credentials = B2AccountCredentials(accountId, applicationKey)

  val testRun = UUID.randomUUID().toString
  val fileName = FileName(s"test-$testRun.txt")

  val dataText = s"This is test data '$testRun'!"
  val dataByteString = ByteString(dataText.getBytes(StandardCharsets.UTF_8.name))
}
