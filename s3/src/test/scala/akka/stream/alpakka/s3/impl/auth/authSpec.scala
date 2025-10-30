/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.s3.impl.auth

import org.scalatest.flatspec.AnyFlatSpec

class authSpec extends AnyFlatSpec {

  "encodeHex" should "encode string to hex string" in {
    assert(encodeHex("1234+abcd".getBytes()) == "313233342b61626364")
  }

}
