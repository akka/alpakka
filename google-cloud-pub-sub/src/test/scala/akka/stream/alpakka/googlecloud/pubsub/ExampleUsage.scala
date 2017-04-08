/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.googlecloud.pubsub

import java.security.{KeyFactory, PrivateKey}
import java.security.spec.PKCS8EncodedKeySpec
import java.util.Base64

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.googlecloud.pubsub.scaladsl.GooglePubSub
import akka.stream.scaladsl.{Flow, Sink, Source}

import scala.collection.immutable.Seq
import scala.concurrent.Future
import scala.concurrent.duration._

class ExampleUsage {

  //#init-mat
  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()
  //#init-mat

  //#init-credentials
  val privateKey: PrivateKey = {
    val pk = "MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCxwdLoCIviW0BsREeKzi" +
    "qiSgzl17Q6nD4RhqbB71oPGG8h82EJPeIlLQsMGEtuig0MVsUa9MudewFuQ/XHWtxnueQ3I900EJm" +
    "rDTA4ysgHcVvyDBPuYdVVV7LE/9nysuHb2x3bh057Sy60qZqDS2hV9ybOBp2RIEK04k/hQDDqp+Lx" +
    "cnNQBi5C0f6aohTN6Ced2vvTY6hWbgFDk4Hdw9JDJpf8TSx/ZxJxPd3EA58SgXRBuamVZWy1IVpFO" +
    "SKUCr4wwMOrELu9mRGzmNJiLSqn1jqJlG97ogth3dEldSOtwlfVI1M4sDe3k1SnF1+IagfK7Wda5h" +
    "PbMdbh2my3EMGY159ktbtTAUzJejPQfhVzk84XNxVPdjN01xN2iceXSKcJHzy8iy9JHb+t9qIIcYk" +
    "ZPJrBCyphUGlMWE+MFwtjbHMBxhqJNyG0TYByWudF+/QRFaz0FsMr4TmksNmoLPBZTo8zAoGBAKZI" +
    "vf5XBlTqd/tR4cnTBQOeeegTHT5x7e+W0mfpCo/gDDmKnOsF2lAwj/F/hM5WqorHoM0ibno+0zUb5" +
    "q6rhccAm511h0LmV1taVkbWk4UReuPuN+UyVUP+IjmXjagDle9IkOE7+fDlNb+Q7BHl2R8zm1jZjE" +
    "DwM2NQnSxQ22+/"
    val kf = KeyFactory.getInstance("RSA")
    val encodedPv = Base64.getDecoder.decode(pk)
    val keySpecPv = new PKCS8EncodedKeySpec(encodedPv)
    kf.generatePrivate(keySpecPv)
  }
  val clientEmail = "test-XXX@test-XXXXX.iam.gserviceaccount.com"
  val projectId = "test-XXXXX"
  val apiKey = "AIzaSyCVvqrlz057gCssc70n5JERyTW4TpB4ebE"
  val topic = "topic1"
  val subscription = "subscription1"
  //#init-credentials

  //#publish-single
  val publishMessage =
    PubSubMessage(messageId = "1", data = new String(Base64.getEncoder.encode("Hello Google!".getBytes)))
  val publishRequest = PublishRequest(Seq(publishMessage))

  val source: Source[PublishRequest, NotUsed] = Source.single(publishRequest)

  val publishFlow: Flow[PublishRequest, Seq[String], NotUsed] =
    GooglePubSub.publish(projectId, apiKey, clientEmail, privateKey, topic)

  val publishedMessageIds: Future[Seq[Seq[String]]] = source.via(publishFlow).runWith(Sink.seq)
  //#publish-single

  //#publish-fast
  val messageSource: Source[PubSubMessage, NotUsed] = Source(List(publishMessage, publishMessage))
  messageSource.groupedWithin(1000, 1.minute).map(PublishRequest.apply).via(publishFlow).to(Sink.seq)
  //#publish-fast

  //#subscribe
  val subscriptionSource: Source[ReceivedMessage, NotUsed] =
    GooglePubSub.subscribe(projectId, apiKey, clientEmail, privateKey, subscription)

  val ackSink: Sink[AcknowledgeRequest, Future[Done]] =
    GooglePubSub.acknowledge(projectId, apiKey, clientEmail, privateKey, subscription)

  subscriptionSource
    .map { message =>
      // do something fun

      message.ackId
    }
    .groupedWithin(1000, 1.minute)
    .map(AcknowledgeRequest.apply)
    .to(ackSink)
  //#subscribe

  //#subscribe-auto-ack
  val subscribeMessageSoruce: Source[ReceivedMessage, NotUsed] = ???
  val processMessage: Sink[ReceivedMessage, NotUsed] = ???

  val batchAckSink =
    Flow[ReceivedMessage].map(_.ackId).groupedWithin(1000, 1.minute).map(AcknowledgeRequest.apply).to(ackSink)

  val q = subscribeMessageSoruce.alsoTo(batchAckSink).to(processMessage)
  //#subscribe-auto-ack

}
