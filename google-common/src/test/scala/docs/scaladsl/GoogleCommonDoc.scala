/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import akka.actor.ActorSystem
import akka.stream.Graph
import akka.stream.alpakka.google.{GoogleAttributes, GoogleSettings}
import akka.stream.scaladsl.Source

import scala.annotation.nowarn

@nowarn
class GoogleCommonDoc {

  implicit val system: ActorSystem = ???
  val stream: Graph[Nothing, Nothing] = ???

  { //#accessing-settings
    val defaultSettings = GoogleSettings()
    val customSettings = GoogleSettings("my-app.custom-google-config")
    Source.fromMaterializer { (mat, attr) =>
      val settings: GoogleSettings = GoogleAttributes.resolveSettings(mat, attr)
      Source.empty
    }
    //#accessing-settings
  }

  {
    //#custom-settings
    stream.addAttributes(GoogleAttributes.settingsPath("my-app.custom-google-config"))

    val defaultSettings = GoogleSettings()
    val customSettings = defaultSettings.withProjectId("my-other-project")
    stream.addAttributes(GoogleAttributes.settings(customSettings))
    //#custom-settings
  }

}
