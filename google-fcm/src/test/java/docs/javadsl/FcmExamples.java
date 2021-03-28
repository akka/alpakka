/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import akka.actor.ActorSystem;
import akka.japi.Pair;
// #imports
import akka.stream.alpakka.google.firebase.fcm.*;
import akka.stream.alpakka.google.firebase.fcm.javadsl.GoogleFcm;

// #imports
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;

import java.util.List;
import java.util.concurrent.CompletionStage;

public class FcmExamples {

  private static void example() {
    ActorSystem system = ActorSystem.create();

    // #simple-send
    FcmSettings fcmConfig = FcmSettings.create();
    FcmNotification notification =
        FcmNotification.basic(
            "Test", "This is a test notification!", new FcmNotificationModels.Token("token"));
    Source.single(notification).runWith(GoogleFcm.fireAndForget(fcmConfig), system);
    // #simple-send

    // #asFlow-send
    CompletionStage<List<FcmResponse>> result1 =
        Source.single(notification)
            .via(GoogleFcm.send(fcmConfig))
            .map(
                res -> {
                  if (res.isSuccess()) {
                    FcmSuccessResponse response = (FcmSuccessResponse) res;
                    System.out.println("Successful " + response.getName());
                  } else {
                    FcmErrorResponse response = (FcmErrorResponse) res;
                    System.out.println("Send error " + response.getRawError());
                  }
                  return res;
                })
            .runWith(Sink.seq(), system);
    // #asFlow-send

    // #withData-send
    CompletionStage<List<Pair<FcmResponse, String>>> result2 =
        Source.single(Pair.create(notification, "superData"))
            .via(GoogleFcm.sendWithPassThrough(fcmConfig))
            .runWith(Sink.seq(), system);
    // #withData-send

  }
}
