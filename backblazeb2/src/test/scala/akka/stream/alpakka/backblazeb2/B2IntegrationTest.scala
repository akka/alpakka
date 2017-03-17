package akka.stream.alpakka.backblazeb2

import akka.stream.alpakka.backblazeb2.Protocol._

trait B2IntegrationTest {
  private def readEnv(key: String): String = {
    Option(System.getenv(key)) getOrElse sys.error(s"Please set $key environment variable to run the tests")
  }

  val accountId = AccountId(readEnv("B2_ACCOUNT_ID"))
  val applicationKey = ApplicationKey(readEnv("B2_APPLICATION_KEY"))
  val bucketName = BucketName("alpakka-test") // TODO: create new using b2_create_bucket then remove after
  val bucketId = BucketId(readEnv("B2_BUCKET_ID")) // TODO: read using b2_list_buckets API call

  val credentials = B2AccountCredentials(accountId, applicationKey)
}
