/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.storage

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.alpakka.googlecloud.storage.Session.GoogleAuthConfiguration
import org.specs2.mutable.Specification

class SessionSpec extends Specification {

  "The session" should {

    "report the path of the missing service account file" in {
      implicit val as: ActorSystem = null
      implicit val mat: Materializer = null
      val session = new Session(GoogleAuthConfiguration("missingpath"), Seq.empty)
      session.getToken() must throwA[RuntimeException]("missingpath")
    }

  }

}
