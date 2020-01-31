/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl
import java.util.logging.{Level, Logger}

import akka.actor.{ActorSystem, Cancellable}
import akka.stream.alpakka.googlecloud.pubsub.grpc.scaladsl.GooglePubSub
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, DelayOverflowStrategy, Materializer}
import com.google.protobuf.ByteString
import com.google.pubsub.v1.pubsub.{PublishRequest, PubsubMessage, StreamingPullRequest}
import com.typesafe.config.ConfigFactory

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

object ExampleApp {

  Logger.getLogger("io.grpc.netty").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {

    val config = ConfigFactory.parseString("""
        |akka.loglevel = INFO
      """.stripMargin)

    implicit val sys = ActorSystem("ExampleApp", config)
    implicit val mat = ActorMaterializer()
    import sys.dispatcher

    val result = args.toList match {
      case "publish-single" :: rest => publishSingle(rest)
      case "publish-stream" :: rest => publishStream(rest)
      case "subscribe" :: rest => subscribeStream(rest)
      case other => Future.failed(new Error(s"unknown arguments: $other"))
    }

    result.onComplete { res =>
      res match {
        case Success(c: Cancellable) =>
          println("### Press ENTER to stop the application.")
          scala.io.StdIn.readLine()
          c.cancel()
        case Success(s) =>
          println(s)
        case Failure(ex) =>
          ex.printStackTrace()
      }
      sys.terminate()
    }
  }

  private def publishSingle(args: List[String])(implicit mat: Materializer) = {
    val projectId :: topic :: Nil = args

    Source
      .single(publish(projectId, topic)("Hello!"))
      .via(GooglePubSub.publish(parallelism = 1))
      .runWith(Sink.head)
  }

  private def publishStream(args: List[String])(implicit mat: Materializer) = {
    val projectId :: topic :: Nil = args

    Source
      .tick(0.seconds, 1.second, ())
      .map(_ => {
        val temp = math.random * 10 + 15
        f"Current temperature is: $temp%2.2f"
      })
      .delay(1.second, DelayOverflowStrategy.backpressure)
      .map(publish(projectId, topic)(_))
      .via(GooglePubSub.publish(parallelism = 1))
      .to(Sink.ignore)
      .mapMaterializedValue(Future.successful(_))
      .run()
  }

  private def subscribeStream(args: List[String])(implicit mat: Materializer) = {
    val projectId :: sub :: Nil = args

    GooglePubSub
      .subscribe(subscribe(projectId, sub), 1.second)
      .to(Sink.foreach(println))
      .run()
  }

  private def publish(projectId: String, topic: String)(msg: String) =
    PublishRequest(topicFqrs(projectId, topic), Seq(PubsubMessage(ByteString.copyFromUtf8(msg))))

  private def subscribe(projectId: String, sub: String) =
    StreamingPullRequest(subFqrs(projectId, sub)).withStreamAckDeadlineSeconds(10)

  private def topicFqrs(projectId: String, topic: String) =
    s"projects/$projectId/topics/$topic"

  private def subFqrs(projectId: String, sub: String) =
    s"projects/$projectId/subscriptions/$sub"

}
