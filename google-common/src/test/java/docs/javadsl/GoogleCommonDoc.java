/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import akka.actor.ActorSystem;
import akka.stream.Graph;
import akka.stream.alpakka.google.GoogleAttributes;
import akka.stream.alpakka.google.GoogleSettings;
import akka.stream.javadsl.Source;

public class GoogleCommonDoc {

  ActorSystem system = null;
  Graph<?, ?> stream = null;

  void accessingSettings() {
    // #accessing-settings
    GoogleSettings defaultSettings = GoogleSettings.create(system);
    GoogleSettings customSettings = GoogleSettings.create("my-app.custom-google-config", system);
    Source.fromMaterializer(
        (mat, attr) -> {
          GoogleSettings settings = GoogleAttributes.resolveSettings(mat, attr);
          return Source.empty();
        });
    // #accessing-settings
  }

  void customSettings() {
    // #custom-settings
    stream.addAttributes(GoogleAttributes.settingsPath("my-app.custom-google-config"));

    GoogleSettings defaultSettings = GoogleSettings.create(system);
    GoogleSettings customSettings = defaultSettings.withProjectId("my-other-project");
    stream.addAttributes(GoogleAttributes.settings(customSettings));
    // #custom-settings
  }
}
