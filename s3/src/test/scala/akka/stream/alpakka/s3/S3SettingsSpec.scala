/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.s3

import akka.stream.alpakka.s3.scaladsl.{S3ClientIntegrationSpec, S3WireMockBase}
import com.amazonaws.auth._
import com.amazonaws.regions.DefaultAwsRegionProviderChain
import com.typesafe.config.ConfigFactory
import org.scalatest.OptionValues

class S3SettingsSpec extends S3WireMockBase with S3ClientIntegrationSpec with OptionValues {
  private def mkConfig(more: String): S3Settings =
    S3Settings(
      ConfigFactory.parseString(
        s"""
          |buffer = memory
          |path-style-access = false
          |$more
        """.stripMargin
      )
    )

  "S3Settings" should "correctly parse config with anonymous credentials" in {
    val settings: S3Settings = mkConfig("aws.credentials.provider = anon")

    settings.credentialsProvider.getCredentials shouldBe a[AnonymousAWSCredentials]
  }

  it should "correctly parse config with static credentials / basic" in {
    val testKi: String = "testki"
    val testSk: String = "testsk"

    val settings: S3Settings = mkConfig(
      s"""aws.credentials {
        | provider = static
        | access-key-id = $testKi
        | secret-access-key = $testSk
        |}
      """.stripMargin
    )
    settings.credentialsProvider.getCredentials shouldBe a[BasicAWSCredentials]
    settings.credentialsProvider.getCredentials.getAWSAccessKeyId shouldBe testKi
    settings.credentialsProvider.getCredentials.getAWSSecretKey shouldBe testSk
  }

  it should "correctly parse config with static credentials / session" in {
    val testKi: String = "testki"
    val testSk: String = "testsk"
    val testTok: String = "testtok"

    val settings: S3Settings = mkConfig(
      s"""aws.credentials {
         | provider = static
         | access-key-id = $testKi
         | secret-access-key = $testSk
         | token = $testTok
         |}
      """.stripMargin
    )
    settings.credentialsProvider.getCredentials shouldBe a[AWSSessionCredentials]
    val creds: AWSSessionCredentials = settings.credentialsProvider.getCredentials.asInstanceOf[AWSSessionCredentials]

    creds.getSessionToken shouldBe testTok
    creds.getAWSAccessKeyId shouldBe testKi
    creds.getAWSSecretKey shouldBe testSk
  }

  it should "correctly parse config with default credentials" in {
    val settings: S3Settings = mkConfig(
      "aws.credentials.provider = default"
    )
    settings.credentialsProvider shouldBe a[DefaultAWSCredentialsProviderChain]
    settings.endpointUrl shouldBe 'empty
  }

  it should "correctly fallback to default credentials provider" in {
    val settings: S3Settings = mkConfig(
      "" // no credentials section
    )
    settings.credentialsProvider shouldBe a[DefaultAWSCredentialsProviderChain]
  }

  it should "use default region provider chain by default" in {
    val settings: S3Settings = mkConfig(
      "" // no credentials section
    )
    settings.s3RegionProvider shouldBe a[DefaultAwsRegionProviderChain]
  }

  it should "use given region when using static region provider" in {
    val otherRegion = "testRegion"

    val settings: S3Settings = mkConfig(
      s"""
         |aws.region.provider = static
         |aws.region.default-region = $otherRegion
         |""".stripMargin
    )
    settings.s3RegionProvider.getRegion shouldBe otherRegion
  }

  it should "use default region provider when set in configuration" in {
    val settings: S3Settings = mkConfig(
      "aws.region.provider = default" // no credentials section
    )
    settings.s3RegionProvider shouldBe a[DefaultAwsRegionProviderChain]
  }

  it should "properly handle a missing endpoint url" in {
    val settings: S3Settings = mkConfig("")
    settings.endpointUrl shouldBe 'empty
  }

  it should "properly handle a null endpoint url" in {
    val settings: S3Settings = mkConfig(
      s"""
         |endpoint-url = null
        """.stripMargin
    )
    settings.endpointUrl shouldBe 'empty
  }

  it should "instantiate with a custom endpoint uri" in {
    val endpointUrl = "http://localhost:9000"

    val settings: S3Settings = mkConfig(
      s"""
           |endpoint-url = "$endpointUrl"
        """.stripMargin
    )
    settings.endpointUrl.value shouldEqual endpointUrl
  }

  it should "be able to instantiate using custom config prefix" in {
    val otherRegion = "testRegion"
    val endpointUrl = "http://localhost:9000"

    val settings: S3Settings = S3Settings(
      ConfigFactory.parseString(
        s"""
           |aws.region.provider = static
           |aws.region.default-region = $otherRegion
           |buffer = memory
           |path-style-access = true
           |endpoint-url = "$endpointUrl"
        """.stripMargin
      )
    )

    settings.pathStyleAccess shouldBe true
    settings.s3RegionProvider.getRegion shouldBe otherRegion
    settings.endpointUrl.value shouldEqual endpointUrl
  }

  it should "instantiate with the list bucket api version 2 by default" in {
    val settings: S3Settings = mkConfig("")
    settings.listBucketApiVersion shouldEqual ApiVersion.ListBucketVersion2
  }

  it should "instantiate with the list bucket api version 1 if list-bucket-api-version is set to 1" in {
    val settings: S3Settings = mkConfig("list-bucket-api-version = 1")
    settings.listBucketApiVersion shouldEqual ApiVersion.ListBucketVersion1
  }

  it should "instantiate with the list bucket api version 2 if list-bucket-api-version is set to a number that is neither 1 or 2" in {
    val settings: S3Settings = mkConfig("list-bucket-api-version = 0")
    settings.listBucketApiVersion shouldEqual ApiVersion.ListBucketVersion2
  }

  it should "instantiate with the list bucket api version 2 if list-bucket-api-version is set to a value that is not a nymber" in {
    val settings: S3Settings = mkConfig("list-bucket-api-version = 'version 1'")
    settings.listBucketApiVersion shouldEqual ApiVersion.ListBucketVersion2
  }
}
