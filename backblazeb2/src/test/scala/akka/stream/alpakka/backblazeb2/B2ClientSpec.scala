/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.backblazeb2

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.backblazeb2.scaladsl.B2Client
import org.scalatest.AsyncFlatSpec
import org.scalatest.Matchers._

class B2ClientSpec extends AsyncFlatSpec {
  val accountId = Option(System.getenv("B2_ACCOUNT_ID")) getOrElse sys.error("Please set B2_ACCOUNT_ID environment variable to run the tests")
  val applicationKey = Option(System.getenv("B2_APPLICATION_KEY")) getOrElse sys.error("Please set B2_APPLICATION_KEY environment variable to run the tests")

  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()

  val client = new B2Client

  it should "authorize account" in {
    val result = client.authorizeAccount(accountId, applicationKey)
    result map { result =>
      result.authorizationToken.value should not be empty
      result.apiUrl should not be empty
    }
  }
}
