/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.s3.impl.auth

import org.scalatest.flatspec.AnyFlatSpec

class authSpec extends AnyFlatSpec {

  "encodeHex" should "encode string to hex string" in {
    assert(encodeHex("1234+abcd".getBytes()) == "313233342b61626364")
  }

}
