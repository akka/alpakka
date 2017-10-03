/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.oracle.bmcs.auth

import org.scalatest.{FlatSpecLike, Matchers}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}

class BmcsCredentialsSpec extends FlatSpecLike with Matchers with ScalaFutures {

  implicit val defaultPatience: PatienceConfig =
    PatienceConfig(timeout = Span(2, Seconds), interval = Span(5, Millis))

  "BmcCredential" should "read RsaPrivate key " in {
    val keyFile: String =
      "./bmcs/src/test/resources/oci_api_key.pem"

    val credentials: BmcsCredentials = BasicCredentials("", "", keyPath = keyFile, Some("adityag"), "")
    //TODO: find better way to ensure key was read.
    credentials.rsaPrivateKey.hashCode should equal(543210)
  }

}
