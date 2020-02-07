/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.s3

import akka.stream.alpakka.s3.scaladsl.{S3ClientIntegrationSpec, S3WireMockBase}
import software.amazon.awssdk.auth.credentials._
import software.amazon.awssdk.regions.providers._
import com.typesafe.config.ConfigFactory
import org.scalatest.OptionValues
import software.amazon.awssdk.regions.Region

class S3SettingsSpec extends S3WireMockBase with S3ClientIntegrationSpec with OptionValues {
  private def mkSettings(more: String): S3Settings =
    S3Settings(
      ConfigFactory.parseString(
        s"""
          |buffer = memory
          |path-style-access = false
          |validate-object-key = true
          |retry-settings {
          |  max-retries-per-chunk = 3
          |  min-backoff = 200ms
          |  max-backoff = 10s
          |  random-factor = 0.0
          |}
          |$more
        """.stripMargin
      )
    )

  "S3Settings" should "correctly parse config with anonymous credentials" in {
    val settings: S3Settings = mkSettings("aws.credentials.provider = anon")

    settings.credentialsProvider.resolveCredentials shouldBe AnonymousCredentialsProvider.create().resolveCredentials()
  }

  it should "correctly parse config with static credentials / basic" in {
    val testKi: String = "testki"
    val testSk: String = "testsk"

    val settings: S3Settings = mkSettings(
      s"""aws.credentials {
        | provider = static
        | access-key-id = $testKi
        | secret-access-key = $testSk
        |}
      """.stripMargin
    )
    settings.credentialsProvider.resolveCredentials() shouldBe a[AwsBasicCredentials]
    settings.credentialsProvider.resolveCredentials().accessKeyId shouldBe testKi
    settings.credentialsProvider.resolveCredentials().secretAccessKey shouldBe testSk
  }

  it should "correctly parse config with static credentials / session" in {
    val testKi: String = "testki"
    val testSk: String = "testsk"
    val testTok: String = "testtok"

    val settings: S3Settings = mkSettings(
      s"""aws.credentials {
         | provider = static
         | access-key-id = $testKi
         | secret-access-key = $testSk
         | token = $testTok
         |}
      """.stripMargin
    )
    settings.credentialsProvider.resolveCredentials() shouldBe a[AwsSessionCredentials]
    val creds: AwsSessionCredentials =
      settings.credentialsProvider.resolveCredentials().asInstanceOf[AwsSessionCredentials]

    creds.sessionToken shouldBe testTok
    creds.accessKeyId shouldBe testKi
    creds.secretAccessKey shouldBe testSk
  }

  it should "correctly parse config with default credentials" in {
    val settings: S3Settings = mkSettings(
      "aws.credentials.provider = default"
    )
    settings.credentialsProvider shouldBe a[DefaultCredentialsProvider]
    settings.endpointUrl shouldBe 'empty
  }

  it should "correctly fallback to default credentials provider" in {
    val settings: S3Settings = mkSettings(
      "" // no credentials section
    )
    settings.credentialsProvider shouldBe a[DefaultCredentialsProvider]
  }

  it should "use default region provider chain by default" in {
    val settings: S3Settings = mkSettings(
      "" // no credentials section
    )
    settings.s3RegionProvider shouldBe a[AwsRegionProvider]
  }

  it should "use given region when using static region provider" in {
    val otherRegion = Region.of("testRegion")

    val settings: S3Settings = mkSettings(
      s"""
         |aws.region.provider = static
         |aws.region.default-region = $otherRegion
         |""".stripMargin
    )
    settings.s3RegionProvider.getRegion shouldBe otherRegion
  }

  it should "use default region provider when set in configuration" in {
    val settings: S3Settings = mkSettings(
      "aws.region.provider = default" // no credentials section
    )
    settings.s3RegionProvider shouldBe a[DefaultAwsRegionProviderChain]
  }

  it should "properly handle a missing endpoint url" in {
    val settings: S3Settings = mkSettings("")
    settings.endpointUrl shouldBe 'empty
  }

  it should "properly handle a null endpoint url" in {
    val settings: S3Settings = mkSettings(
      s"""
         |endpoint-url = null
        """.stripMargin
    )
    settings.endpointUrl shouldBe 'empty
  }

  it should "instantiate with a custom endpoint uri" in {
    val endpointUrl = "http://localhost:9000"

    val settings: S3Settings = mkSettings(
      s"""
           |endpoint-url = "$endpointUrl"
        """.stripMargin
    )
    settings.endpointUrl.value shouldEqual endpointUrl
  }

  it should "be able to instantiate using custom config prefix" in {
    val otherRegion = Region.of("testRegion")
    val endpointUrl = "http://localhost:9000"

    val settings: S3Settings = mkSettings(
      s"""
           |aws.region.provider = static
           |aws.region.default-region = $otherRegion
           |buffer = memory
           |path-style-access = true
           |endpoint-url = "$endpointUrl"
        """
    )

    settings.pathStyleAccess shouldBe true
    settings.s3RegionProvider.getRegion shouldBe otherRegion
    settings.endpointUrl.value shouldEqual endpointUrl
  }

  it should "instantiate with the list bucket api version 2 by default" in {
    val settings: S3Settings = mkSettings("")
    settings.listBucketApiVersion shouldEqual ApiVersion.ListBucketVersion2
  }

  it should "instantiate with the list bucket api version 1 if list-bucket-api-version is set to 1" in {
    val settings: S3Settings = mkSettings("list-bucket-api-version = 1")
    settings.listBucketApiVersion shouldEqual ApiVersion.ListBucketVersion1
  }

  it should "instantiate with the list bucket api version 2 if list-bucket-api-version is set to a number that is neither 1 or 2" in {
    val settings: S3Settings = mkSettings("list-bucket-api-version = 0")
    settings.listBucketApiVersion shouldEqual ApiVersion.ListBucketVersion2
  }

  it should "instantiate with the list bucket api version 2 if list-bucket-api-version is set to a value that is not a number" in {
    val settings: S3Settings = mkSettings("list-bucket-api-version = 'version 1'")
    settings.listBucketApiVersion shouldEqual ApiVersion.ListBucketVersion2
  }

  it should "parse forward proxy without credentials" in {
    val settings = mkSettings("""
        |forward-proxy {
        |  host = proxy-host
        |  port = 1337
        |}
      """.stripMargin)

    settings.forwardProxy.value.host shouldEqual "proxy-host"
    settings.forwardProxy.value.port shouldEqual 1337
    settings.forwardProxy.value.credentials shouldBe 'empty
  }

  it should "parse forward proxy with credentials" in {
    val settings = mkSettings("""
                                |forward-proxy {
                                |  host = proxy-host
                                |  port = 1337
                                |  credentials {
                                |    username = knock-knock
                                |    password = whos-there
                                |  }
                                |}
                              """.stripMargin)

    settings.forwardProxy.value.host shouldEqual "proxy-host"
    settings.forwardProxy.value.port shouldEqual 1337
    settings.forwardProxy.value.credentials.value.username shouldEqual "knock-knock"
    settings.forwardProxy.value.credentials.value.password shouldEqual "whos-there"
  }
}
