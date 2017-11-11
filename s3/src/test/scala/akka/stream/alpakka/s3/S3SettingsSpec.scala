/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.s3

import akka.stream.alpakka.s3.scaladsl.{S3ClientIntegrationSpec, S3WireMockBase}
import com.amazonaws.auth._
import com.typesafe.config.ConfigFactory

class S3SettingsSpec extends S3WireMockBase with S3ClientIntegrationSpec {
  private def mkConfig(more: String): S3Settings =
    S3Settings.apply(
      ConfigFactory.parseString(
        s"""
          |akka.stream.alpakka.s3.buffer = memory
          |akka.stream.alpakka.s3.aws.default-region = us-east-1
          |akka.stream.alpakka.s3.path-style-access = false
          |$more
        """.stripMargin
      )
    )

  "S3Settings" should "correctly parse config with anonymous credentials" in {
    val settings: S3Settings = mkConfig("akka.stream.alpakka.s3.aws.credentials.provider = anon")

    settings.credentialsProvider.getCredentials shouldBe a[AnonymousAWSCredentials]
  }

  it should "correctly parse config with static credentials / basic" in {
    val testKi: String = "testki"
    val testSk: String = "testsk"

    val settings: S3Settings = mkConfig(
      s"""akka.stream.alpakka.s3.aws.credentials {
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
      s"""akka.stream.alpakka.s3.aws.credentials {
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
      "akka.stream.alpakka.s3.aws.credentials.provider = default"
    )
    settings.credentialsProvider shouldBe a[DefaultAWSCredentialsProviderChain]
  }

  it should "correctly fallback to default credentials provider" in {
    val settings: S3Settings = mkConfig(
      "" // no credentials section
    )
    settings.credentialsProvider shouldBe a[DefaultAWSCredentialsProviderChain]
  }

  it should "fall back to old / deprecated credentials if new config is empty, but old is present" in {
    val testAki = "test-aki"
    val testSak = "test-sak"
    val settings = mkConfig(
      s"""akka.stream.alpakka.s3.aws.access-key-id = $testAki
         |akka.stream.alpakka.s3.aws.secret-access-key = $testSak""".stripMargin
    )
    settings.credentialsProvider shouldBe a[AWSStaticCredentialsProvider]
    settings.credentialsProvider.getCredentials shouldBe a[BasicAWSCredentials]
    settings.credentialsProvider.getCredentials.getAWSAccessKeyId shouldBe testAki
    settings.credentialsProvider.getCredentials.getAWSSecretKey shouldBe testSak
  }
}
