package akka.stream.alpakka
package azure
package storage

import com.typesafe.config.ConfigFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scala.concurrent.duration._

import scala.util.{Failure, Success, Try}

class StorageSettingsSpec extends AnyWordSpec with Matchers {

  private def mkSettings(more: String) =
    StorageSettings(
      ConfigFactory
        .parseString(more)
        .withFallback(ConfigFactory.parseString(s"""
                                                   |api-version = "2024-11-04"
                                                   |signing-algorithm = "HmacSHA256"
                                                   |credentials {
                                                   |  authorization-type = anon
                                                   |  account-name = mystorageaccount
                                                   |  account-key = bXlhY2NvdW50a2V5
                                                   |  sas-token = ""
                                                   |}
                                                   |retry-settings {
                                                   |  max-retries = 5
                                                   |  min-backoff = 300ms
                                                   |  max-backoff = 5s
                                                   |  random-factor = 0.3
                                                   |}
                                                   |""".stripMargin))
        .resolve()
    )

  "StorageSettings" should {
    "correctly parse config with anonymous authorization type" in {
      val settings = mkSettings("")
      settings.authorizationType shouldBe "anon"
    }

    "correctly parse config with SharedKey authorization type" in {
      val settings = mkSettings("credentials.authorization-type = SharedKey")
      settings.authorizationType shouldBe "SharedKey"
    }

    "correctly parse config with SharedKeyLite authorization type" in {
      val settings = mkSettings("credentials.authorization-type = SharedKeyLite")
      settings.authorizationType shouldBe "SharedKeyLite"
    }

    "correctly parse config with sas authorization type" in {
      val settings = mkSettings("credentials.authorization-type = sas")
      settings.authorizationType shouldBe "sas"
    }

    "fallback to anonymous authorization type when incorrectly configured" in {
      val settings = mkSettings("credentials.authorization-type = dummy")
      settings.authorizationType shouldBe "anon"
    }

    "successfully parse account name and key" in {
      val expected = AzureNameKeyCredential("mystorageaccount", "bXlhY2NvdW50a2V5")
      val settings = mkSettings("")
      val actual = settings.azureNameKeyCredential
      actual shouldBe a[AzureNameKeyCredential]
      actual.accountName shouldBe expected.accountName
      actual.accountKey shouldEqual expected.accountKey
    }

    "throw RuntimeException if credentials are missing" in {
      Try(StorageSettings(ConfigFactory.parseString(s"""
           |api-version = "2024-11-04"
           |signing-algorithm = "HmacSHA256"
           |
           |retry-settings {
           |  max-retries = 3
           |  min-backoff = 200ms
           |  max-backoff = 10s
           |  random-factor = 0.0
           |}
           |""".stripMargin).resolve())) match {
        case Failure(ex) =>
          ex shouldBe a[RuntimeException]
          ex.getMessage shouldBe "credentials must be defined."
        case Success(_) => fail("Should have thrown exception")
      }
    }

    "throw RuntimeException if account name is empty" in {
      Try(mkSettings(""" credentials.account-name="" """)) match {
        case Failure(ex) =>
          ex shouldBe a[RuntimeException]
          ex.getMessage shouldBe "accountName property must be defined"
        case Success(_) => fail("Should have thrown exception")
      }
    }

    "parse retry settings" in {
      val settings = mkSettings("")
      settings.retrySettings shouldBe RetrySettings(maxRetries = 5,
                                                    minBackoff = 300.milliseconds,
                                                    maxBackoff = 5.seconds,
                                                    randomFactor = 0.3)
    }

    "use default retry settings if config is missing" in {
      val settings = StorageSettings(ConfigFactory.parseString(s"""
                                                                  |api-version = "2024-11-04"
                                                                  |signing-algorithm = "HmacSHA256"
                                                                  |credentials {
                                                                  |  authorization-type = anon
                                                                  |  account-name = mystorageaccount
                                                                  |  account-key = bXlhY2NvdW50a2V5
                                                                  |  sas-token = ""
                                                                  |}
                                                                  |
                                                                  |""".stripMargin).resolve())
      settings.retrySettings shouldBe RetrySettings.Default
    }

    "use default retry settings if config is misconfigured" in {
      val settings = mkSettings("retry-settings.min-backoff=hello")
      settings.retrySettings shouldBe RetrySettings.Default
    }

    "populate endpoint URL if provided" in {
      val settings = mkSettings("""endpoint-url="http://localhost:1234" """)
      settings.endPointUrl shouldBe defined
      settings.endPointUrl.get shouldBe "http://localhost:1234"
    }
  }
}
