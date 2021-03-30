/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import akka.actor.ActorSystem;

// #imports
import akka.stream.alpakka.huawei.pushkit.*;
import akka.stream.alpakka.huawei.pushkit.javadsl.HmsPushKit;
import akka.stream.alpakka.huawei.pushkit.models.AndroidConfig;
import akka.stream.alpakka.huawei.pushkit.models.AndroidNotification;
import akka.stream.alpakka.huawei.pushkit.models.BasicNotification;
import akka.stream.alpakka.huawei.pushkit.models.ClickAction;
import akka.stream.alpakka.huawei.pushkit.models.ErrorResponse;
import akka.stream.alpakka.huawei.pushkit.models.PushKitNotification;
import akka.stream.alpakka.huawei.pushkit.models.PushKitResponse;
import akka.stream.alpakka.huawei.pushkit.models.Response;
import akka.stream.alpakka.huawei.pushkit.models.Tokens;

// #imports
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import scala.collection.immutable.Set;

import java.util.List;
import java.util.concurrent.CompletionStage;

public class PushKitExamples {

  public static void example() {
    ActorSystem system = ActorSystem.create();

    // #simple-send
    HmsSettings config = HmsSettings.create(system);
    PushKitNotification notification =
        PushKitNotification.fromJava()
            .withNotification(BasicNotification.fromJava().withTitle("title").withBody("body"))
            .withAndroidConfig(
                AndroidConfig.fromJava()
                    .withNotification(
                        AndroidNotification.fromJava()
                            .withClickAction(ClickAction.fromJava().withType(3))))
            .withTarget(new Tokens(new Set.Set1<>("token").toSeq()));

    Source.single(notification).runWith(HmsPushKit.fireAndForget(config), system);
    // #simple-send

    // #asFlow-send
    CompletionStage<List<Response>> result =
        Source.single(notification)
            .via(HmsPushKit.send(config))
            .map(
                res -> {
                  if (!res.isFailure()) {
                    PushKitResponse response = (PushKitResponse) res;
                    System.out.println("Response" + response);
                  } else {
                    ErrorResponse response = (ErrorResponse) res;
                    System.out.println("Send error " + response);
                  }
                  return res;
                })
            .runWith(Sink.seq(), system);
    // #asFlow-send
  }
}
