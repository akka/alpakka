/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.storage

import java.nio.file.Paths

import org.scalatest.WordSpec

class GoogleAuthConfigurationSpec extends WordSpec {

  "The session" should {

    "report the path of the missing service account file" in {
      val thrown = intercept[RuntimeException] {
        GoogleAuthConfiguration.apply(Paths.get("/missing/path/test.json"))
      }
      assert(thrown.getMessage === "Service account file missing: /missing/path/test.json")
    }

  }

}
