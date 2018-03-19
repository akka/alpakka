/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.google.firebase.fcm.javadsl;

//#imports
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.alpakka.google.firebase.fcm.FcmFlowModels;
import akka.stream.alpakka.google.firebase.fcm.FcmNotification;
import akka.stream.alpakka.google.firebase.fcm.FcmNotificationModels;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
//#imports

import java.util.List;
import java.util.concurrent.CompletionStage;

public class Examples {

    private static void example() {
        //#init-mat
        ActorSystem system = ActorSystem.create();
        ActorMaterializer materializer = ActorMaterializer.create(system);
        //#init-mat

        //#init-credentials
        String privateKey =
                        "-----BEGIN RSA PRIVATE KEY-----\n" +
                        "MIIBOgIBAAJBAJHPYfmEpShPxAGP12oyPg0CiL1zmd2V84K5dgzhR9TFpkAp2kl2\n" +
                        "9BTc8jbAY0dQW4Zux+hyKxd6uANBKHOWacUCAwEAAQJAQVyXbMS7TGDFWnXieKZh\n" +
                        "Dm/uYA6sEJqheB4u/wMVshjcQdHbi6Rr0kv7dCLbJz2v9bVmFu5i8aFnJy1MJOpA\n" +
                        "2QIhAPyEAaVfDqJGjVfryZDCaxrsREmdKDlmIppFy78/d8DHAiEAk9JyTHcapckD\n" +
                        "uSyaE6EaqKKfyRwSfUGO1VJXmPjPDRMCIF9N900SDnTiye/4FxBiwIfdynw6K3dW\n" +
                        "fBLb6uVYr/r7AiBUu/p26IMm6y4uNGnxvJSqe+X6AxR6Jl043OWHs4AEbwIhANuz\n" +
                        "Ay3MKOeoVbx0L+ruVRY5fkW+oLHbMGtQ9dZq7Dp9\n" +
                        "-----END RSA PRIVATE KEY-----";
        String clientEmail = "test-XXX@test-XXXXX.iam.gserviceaccount.com";
        String projectId = "test-XXXXX";
        FcmFlowModels.FcmFlowConfig fcmConfig = new FcmFlowModels.FcmFlowConfig(clientEmail, privateKey, projectId, false, 100);
        //#init-credentials

        //#simple-send
        FcmNotification notification = FcmNotification.basic("Test", "This is a test notification!", new FcmNotificationModels.Token("token"));
        Source.single(notification).runWith(GoogleFcmSink.fireAndForget(fcmConfig, system, materializer), materializer);
        //#simple-send

        //#asFlow-send
        CompletionStage<List<FcmFlowModels.FcmResponse>> result1 = Source.single(notification).via(GoogleFcmFlow.send(fcmConfig, system, materializer)).runWith(Sink.seq(), materializer);
        //#asFlow-send

        //#withData-send
        CompletionStage<List<Pair<FcmFlowModels.FcmResponse, String>>> result2 =
                Source.single(new Pair<FcmNotification, String>(notification, "superData"))
                        .via(GoogleFcmFlow.sendWithPassThrough(fcmConfig, system, materializer))
                        .runWith(Sink.seq(), materializer);
        //#withData-send


    }
}
