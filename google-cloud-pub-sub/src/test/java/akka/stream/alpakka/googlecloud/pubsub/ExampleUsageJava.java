/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.pubsub;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.alpakka.googlecloud.pubsub.javadsl.GooglePubSub;
import akka.stream.javadsl.*;
import com.google.common.collect.Lists;

import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CompletionStage;

import scala.concurrent.duration.FiniteDuration;

public class ExampleUsageJava {

    private static void example() throws NoSuchAlgorithmException, InvalidKeySpecException {

        //#init-mat
        ActorSystem system = ActorSystem.create();
        ActorMaterializer materializer = ActorMaterializer.create(system);
        //#init-mat

        //#init-credentials
        String keyString = "MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCxwdLoCIviW0BsREeKzi" +
                "qiSgzl17Q6nD4RhqbB71oPGG8h82EJPeIlLQsMGEtuig0MVsUa9MudewFuQ/XHWtxnueQ3I900EJm" +
                "rDTA4ysgHcVvyDBPuYdVVV7LE/9nysuHb2x3bh057Sy60qZqDS2hV9ybOBp2RIEK04k/hQDDqp+Lx" +
                "cnNQBi5C0f6aohTN6Ced2vvTY6hWbgFDk4Hdw9JDJpf8TSx/ZxJxPd3EA58SgXRBuamVZWy1IVpFO" +
                "SKUCr4wwMOrELu9mRGzmNJiLSqn1jqJlG97ogth3dEldSOtwlfVI1M4sDe3k1SnF1+IagfK7Wda5h" +
                "PbMdbh2my3EMGY159ktbtTAUzJejPQfhVzk84XNxVPdjN01xN2iceXSKcJHzy8iy9JHb+t9qIIcYk" +
                "ZPJrBCyphUGlMWE+MFwtjbHMBxhqJNyG0TYByWudF+/QRFaz0FsMr4TmksNmoLPBZTo8zAoGBAKZI" +
                "vf5XBlTqd/tR4cnTBQOeeegTHT5x7e+W0mfpCo/gDDmKnOsF2lAwj/F/hM5WqorHoM0ibno+0zUb5" +
                "q6rhccAm511h0LmV1taVkbWk4UReuPuN+UyVUP+IjmXjagDle9IkOE7+fDlNb+Q7BHl2R8zm1jZjE" +
                "DwM2NQnSxQ22+/";
        KeyFactory kf = KeyFactory.getInstance("RSA");
        byte[] encodedPv = Base64.getDecoder().decode(keyString);
        PKCS8EncodedKeySpec keySpecPv = new PKCS8EncodedKeySpec(encodedPv);
        PrivateKey privateKey = kf.generatePrivate(keySpecPv);
        String clientEmail = "test-XXX@test-XXXXX.iam.gserviceaccount.com";
        String projectId = "test-XXXXX";
        String apiKey = "AIzaSyCVvqrlz057gCssc70n5JERyTW4TpB4ebE";
        String topic = "topic1";
        String subscription = "subscription1";
        //#init-credentials

        //#publish-single
        PubSubMessage publishMessage =
                new PubSubMessage("1", new String(Base64.getEncoder().encode("Hello Google!".getBytes())));
        PublishRequest publishRequest = PublishRequest.of(Lists.newArrayList(publishMessage));

        Source<PublishRequest, NotUsed> source = Source.single(publishRequest);

        Flow<PublishRequest, List<String>, NotUsed> publishFlow = GooglePubSub.publish(projectId, apiKey, clientEmail, privateKey, topic, 1, system, materializer);

        CompletionStage<List<List<String>>> publishedMessageIds = source.via(publishFlow).runWith(Sink.seq(), materializer);
        //#publish-single

        //#publish-fast
        Source<PubSubMessage, NotUsed> messageSource = Source.single(publishMessage);
        messageSource.groupedWithin(1000, FiniteDuration.apply(1, "min"))
                .map(messages -> PublishRequest.of(messages))
                .via(publishFlow)
                .runWith(Sink.ignore(), materializer);
        //#publish-fast


        //#subscribe
        Source<ReceivedMessage, NotUsed> subscriptionSource = GooglePubSub.subscribe(projectId, apiKey, clientEmail, privateKey, subscription, system);

        Sink<AcknowledgeRequest, CompletionStage<Done>> ackSink = GooglePubSub.acknowledge(projectId, apiKey, clientEmail, privateKey, subscription, 1, system, materializer);

        subscriptionSource.map( message -> {
            // do something fun
            return message.ackId();
         }).groupedWithin(1000, FiniteDuration.apply(1, "min")).map(acks -> AcknowledgeRequest.of(acks)).to(ackSink);
        //#subscribe


        Sink<ReceivedMessage, CompletionStage<Done>> yourProcessingSink = Sink.ignore();

        //#subscribe-auto-ack
        Sink<ReceivedMessage, CompletionStage<Done>> processSink = yourProcessingSink;

        Sink<ReceivedMessage, NotUsed> batchAckSink = Flow.of(ReceivedMessage.class)
                .map(t -> t.ackId())
                .groupedWithin(1000, FiniteDuration.apply(1, "minute"))
                .map(ids -> AcknowledgeRequest.of(ids))
                .to(ackSink);

        subscriptionSource.alsoTo(batchAckSink).to(processSink);
        //#subscribe-auto-ack
    }

}
