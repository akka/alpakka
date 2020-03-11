/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
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
    ActorMaterializer materializer = ActorMaterializer.create(system);

    // #init-credentials
    String privateKey =
        "-----BEGIN RSA PRIVATE KEY-----\n"
            + "MIIBOgIBAAJBAJHPYfmEpShPxAGP12oyPg0CiL1zmd2V84K5dgzhR9TFpkAp2kl2\n"
            + "9BTc8jbAY0dQW4Zux+hyKxd6uANBKHOWacUCAwEAAQJAQVyXbMS7TGDFWnXieKZh\n"
            + "Dm/uYA6sEJqheB4u/wMVshjcQdHbi6Rr0kv7dCLbJz2v9bVmFu5i8aFnJy1MJOpA\n"
            + "2QIhAPyEAaVfDqJGjVfryZDCaxrsREmdKDlmIppFy78/d8DHAiEAk9JyTHcapckD\n"
            + "uSyaE6EaqKKfyRwSfUGO1VJXmPjPDRMCIF9N900SDnTiye/4FxBiwIfdynw6K3dW\n"
            + "fBLb6uVYr/r7AiBUu/p26IMm6y4uNGnxvJSqe+X6AxR6Jl043OWHs4AEbwIhANuz\n"
            + "Ay3MKOeoVbx0L+ruVRY5fkW+oLHbMGtQ9dZq7Dp9\n"
            + "-----END RSA PRIVATE KEY-----";
    String clientEmail = "test-XXX@test-XXXXX.iam.gserviceaccount.com";
    String projectId = "test-XXXXX";
    FcmSettings fcmConfig = FcmSettings.create(clientEmail, privateKey, projectId);
    // #init-credentials

    // #simple-send
    FcmNotification notification =
        FcmNotification.basic(
            "Test", "This is a test notification!", new FcmNotificationModels.Token("token"));
    Source.single(notification).runWith(GoogleFcm.fireAndForget(fcmConfig), materializer);
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
            .runWith(Sink.seq(), materializer);
    // #asFlow-send

    // #withData-send
    CompletionStage<List<Pair<FcmResponse, String>>> result2 =
        Source.single(Pair.create(notification, "superData"))
            .via(GoogleFcm.sendWithPassThrough(fcmConfig))
            .runWith(Sink.seq(), materializer);
    // #withData-send

  }
}
