/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.s3.impl.auth

import org.scalatest.FlatSpec

class authSpec extends FlatSpec {

  "encodeHex" should "encode string to hex string" in {
    assert(encodeHex("1234+abcd".getBytes()) == "313233342b61626364")
  }

}
