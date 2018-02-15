/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.storage

import java.nio.file.Paths

import org.scalatest.WordSpec

class GoogleAuthConfigurationSpec extends WordSpec {

  "The session" should {

    "report the path of the missing service account file" in {
      val testPath = Paths.get(System.getProperty("java.io.tmpdir")).resolve("idonotexist.json")
      val thrown = intercept[RuntimeException] {
        GoogleAuthConfiguration.apply(testPath)
      }
      assert(thrown.getMessage === s"Service account file missing: ${testPath.toAbsolutePath}")
    }

  }

}
