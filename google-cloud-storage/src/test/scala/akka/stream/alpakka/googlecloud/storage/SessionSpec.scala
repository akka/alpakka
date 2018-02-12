/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.storage

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.alpakka.googlecloud.storage.impl.Session
import org.scalatest.WordSpec

class SessionSpec extends WordSpec {

  "The session" should {

    "report the path of the missing service account file" in {
      implicit val as: ActorSystem = null
      implicit val mat: Materializer = null
      val session = new Session(GoogleAuthConfiguration("missingpath"), Seq.empty)
      val thrown = intercept[RuntimeException] {
        session.getToken()
      }
      assert(thrown.getMessage === "Service account file missing: /Users/francisdb/workspace/alpakka/missingpath")
    }

  }

}
